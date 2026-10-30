# SidePluginRepo 的 ColumnFamilyHandle 所有权

## 1. 问题

`DBImpl::CreateColumnFamilyImpl` 的 scope exit 调用 `MaybeRetainCF()`，easy migrate 下
把**刚创建、即将交给调用者的那个 handle** 登记进 `DB_MultiCF_Impl`。调用者按 rocksdb
的约定 `delete` 它之后，注册表里留下的是同一个地址的悬空指针：

- `CloseOneDB()/CloseAllDB()` 遍历 `cf_handles` → `del_rocks(cfh)` → 对已释放内存再
  `delete`；
- 该地址若被新的 `ColumnFamilyData`/handle 复用，`cfh_to_view` 查到别的 view，报
  `ROCKSDB_VERIFY_F(... "cfh must in cfh_to_view")` 后 abort。

用户 destroy handle 是完全合法的行为，缺陷在于注册表登记了不属于自己的 handle。

修复后的语义是「注册表登记 clone，用户那一个归用户」：调用者销毁自己的 handle 与注册表无关。
handle 的死不是 CF 的死——destroy 只是放弃一份引用，CF 还在库里——所以注册表登记和
properties view 都跟着 CF 走，到 close 时统一摘除；要 forget 的只剩 drop，由
`DBImpl::DropColumnFamily()` 成功后的 `MaybeForgetCF()` 负责。

## 2. 修改

一条实质语义变更：**注册表登记 clone，用户那一个归用户**。此外
`DB_MultiCF::CreateColumnFamily*` 的返回值也改成用户的（repo 只留 clone），这是
「repo 返回的 cfh 用户不得 delete」的一处例外。

| 位置 | 改动 | 为什么必须 |
|---|---|---|
| `MaybeRetainCF()`（builtin_plugin_misc.cc） | `DoRegisterColumnFamily(cfh->CloneHandle())` | 缺陷本身：登记 clone，调用者那份归调用者 |
| `DB_MultiCF_Impl(repo, name, db, cf_handles)` ctor | `this->cf_handles = cf_handles;` 之后就地逐 handle `CloneHandle()`，登记循环因此读 `this->cf_handles`（clone） | `Put` 把同一批 handle 同时给了调用者和 repo（easy migrate 的 `MaybeRetainDB`、JNI 的 `putDB`）；不 clone 就会再次出现"用户删、repo 也删" |
| `CloseOneDB` / `CloseAllDB` | `del_rocks(cfh)` → `delete cfh` | registry handle 归 repo，`del_rocksdb_objs` 只该管 DB 对象。留着不删，其 `ColumnFamilyData` 引用会活过 DB，`~ColumnFamilySet()` 的 `assert(last_ref)`/循环删不干净 |
| `db_mtx`（side_plugin_factory.h） | `std::mutex` → `std::recursive_mutex`；锁点写 `std::lock_guard lock(...)`（C++17 CTAD，不写死 mutex 类型） | `CloseOneDB(db,true)`/`CloseAllDB(true)` 在持 `db_mtx` 时删 DB：`~DBImpl → MaybeForgetDB` 会重入同一把锁，非递归时自锁（改前实测 rc=124） |
| `CloseOneDB()` / `CloseAllDB()` | 删 DB 对象**之前**先把该 DB 从 `db.name2p` / `db.p2name` 摘掉 | 递归锁只解决死锁，不解决重入本身的语义：内层 `CloseOneDB()` 会对着外层已 `delete` 的 `cf_handles` 再删一遍。先注销后删除，重入查不到即返回（顶替原来的 TLS 标志） |
| `UnRegisterColumnFamily()` | 按 **名字** 找 registry handle；出锁后 `delete` 它，并清掉该 CF 的 properties view；查不到时若 `cfh->cfd()->IsDropped()` 则静默返回，不 dropped 才打 WARN | 现在唯一的调用者是 `DBImpl::DropColumnFamily()`（成功的 drop 才调），`cfh` 与 registry handle 不是同一个对象，只能按名字找；只摘除而不删则泄漏一个引用已 drop 的 cfd 的 handle；view 不清则永久留在 `props` 里且指向已释放的 handle。未命中只发生在 `DB_MultiCF_Impl::DropColumnFamily` 先摘表、再调 core drop 的那条路径上，此时 CF 已 dropped |
| `~ColumnFamilyHandleImpl()` | 不改，不挂 forget 钩子 | handle 的死不是 CF 的死：调用者 destroy 自己的 handle 只是放弃一份引用，CF 还在库里，注册表登记和 view 都该跟着 CF 走到 close。挂析构钩子会在「用户 destroy handle」这条合法路径上去注销一个仍然活着的 CF，还会把它的 view 一起摘掉 |
| `Impl_Put()`（side_plugin_repo.cc） | 整个登记过程持 `db_mtx`；登记成功后为 `p.dbm` 的每个 cfh（纯 DB 则为其 default CF）建 properties view；重名或同一 db 换名再 `Put` 直接 `ROCKSDB_DIE` | `CloseOneDB()` 在 `db_mtx` 下遍历 `props.p2name` 建快照，而 `DB::Open` 的登记路径原来不加任何锁：并发 insert 会让遍历漏节点，`del_view()` 随即报 `cfh must in cfh_to_view`。最初用不销毁任何 handle 的独立 repro 靠它复现，但那种用法会先撞上 `ColumnFamilySet::~ColumnFamilySet()` 的 `last_ref` 断言（只在 NDEBUG 构建里被编掉），所以 `cfh_close_test` 只保留销毁 handle 的形态。view 必须在 db 挂进 `db.name2p` **之后**建，`Json_dbname()` 才查得到；重名覆盖则会让旧 dbm/cf_handles/view 留在表里，而 `CloseAllDB` 按 name2p 遍历，同一个 db 会被释放两次 |
| `DB_MultiCF_Impl` 参数化 ctor | 只 clone + `AddOneCF_ToMap`；不写 `props`、不持锁 | ctor 里建 view 时 DB 还没挂进 `db.name2p`，`Json_dbname()` 查不到自己；`Impl_PutTpl` 因重名 Die 时也会留下孤儿 view。构造中的对象没有被任何线程看见，不需要锁 |
| `MaybeRetainCF()` / `MaybeForgetCF()` | `dbp.dbm == nullptr`（纯 DB）时直接返回 | `Put(name, DB*)` 只登记 default CF，没有 dbm，解引用即空指针 |
| `DB_MultiCF_Impl::Get()` | 取 `m_mtx` 共享锁 | easy migrate 下 `UnRegisterColumnFamily()` 会在别的线程 erase 同一个 `name2p`；`get_cf_handles_view()` 本来就持锁，Get 是例外 |
| `DB_MultiCF_Impl::DropColumnFamily(cfh)` | 转发到按名字的重载；被删的永远是 registry 的 handle | 原来按 handle 的重载只在 `my_cfh != cfh` 时删 registry handle，clone 泄漏且攥着已 drop 的 cfd |
| `DropColumnFamily(cfname)` | 检查 `db->DropColumnFamily()` 的返回值，失败只打印错误，登记表照旧摘除、返回 OK | 不改语义，只让失败可见（注册表与 DB 状态分叉是既有问题） |
| `CloseOneDB()` / `CloseAllDB()` | 遍历、摘除 `props` 时持 `props_mtx`（遍历取共享锁，`del_view()` 取写锁） | 读侧（属性 dump）和 `Add/RemoveCFPropertiesWebView` 都用 `props_mtx`，只有这里没持，并发 dump 时会读到正在 erase 的 map |
| `DB_MultiCF_Impl::CreateColumnFamily` / `CreateColumnFamilyWithImport` | 登记 `cfh1->CloneHandle()`，**原样返回** `cfh1`；easy migrate 下（`m_repo` 就是 easy repo）核心钩子已经 clone 并登记，包装直接返回、不再 clone | 这两个接口返回的是 rocksdb 语义的 handle，调用者会 `delete`。原来把同一个 pointer 既登记进 `cf_handles` 又交给调用者：调用者一删，注册表里就是悬空指针，`CloseAllDB` 再删一次 / `del_view` 撞 `cfh must in cfh_to_view`。clone 之后「repo 的 cfh 用户不得 delete」多一个例外：这个返回值归用户。easy migrate 下不跳过则同一个 cfd 被登记两份（核心钩子一份、这里一份），`remove_if` 的 `verify(rm_iter + 1 == end)` 直接失败 |
| `DB_MultiCF::DropColumnFamily(cfname)` / `(cfh)` | repo 只销毁 registry 里的那个 handle，并把该 CF 的 properties view（`RemoveCFPropertiesWebView`）一并摘掉。两个重载都**没有 `del_cfh`**：按名字的 handle 调用者根本看不到；按 handle 传入的是调用者自己的或 registry 的，前者归调用者、后者归 repo，没有"删不删"的余地 | 「从 repo create 的 cf，必须使用 repo 的 drop」：registry handle 调用者根本拿不到，原来按 handle 的重载（JNI `dropCF` 就是）不删它，clone 就成了孤儿——NDEBUG 下是泄漏，dbg 下 `~ColumnFamilySet()` 的 `assert(last_ref)` 直接 abort；properties view 也会因此指向已释放的 handle |
| JNI `CreateJDB` | `nativeOpenDBMultiCF` 传进来的 `dbm->cf_handles`（repo 的）逐个 `CloneHandle()`，只把 clone 交给 `RocksDB.fromNativeHandles()` | `fromNativeHandles()` 把收到的每个 handle 放进 `ownedColumnFamilyHandles`，`RocksDB.close()` 会先关它们再 `closeDatabase()`。直接给 repo 的 handle 就是让 Java 去删 repo 的东西 |
| JNI `nativeCreateCF` / `nativeCreateCFWithImport` | 直接返回 `CreateColumnFamily*` 给的那个 handle，不再套一层 clone | 现在该 handle 本来就归调用者，Java `close()` 它就是对的；再 clone 会把中间那个 handle 漏掉 |

`DB_MultiCF::Get` / `get_cf_handles_view` 不改：它们返回的仍是 repo 自己的 handle，调用者
不得 delete（repo 世界的既有约定）。

## 3. 各路径

- **easy：`DB::Open`** → `MaybeRetainDB` → `Put` → ctor clone。用户拿到 core handles 自己
  destroy；`rocksdb_close` → `~DBImpl` → `MaybeForgetDB` → `CloseOneDB(db,false)` → 删
  registry clones + view。
- **easy：`db->CreateColumnFamily`** → `MaybeRetainCF` 登记 clone，返回 core handle（用户的）。
- **easy：用户 `delete h` / `DestroyColumnFamilyHandle` / `rocksdb_column_family_handle_destroy`**
  → 只是一个 handle 的死，注册表登记、`<db>/<cf>` 视图都不动；registry clone 在 close 时由
  `CloseOneDB`/`CloseAllDB` 删除，`ColumnFamilySet` 也到那时才放掉这个 CF。
- **easy：简单 `DB::Open`** → `MaybeRetainDB` 登记 clone，紧接着 core 删掉自己那个从没交给调用者
  的 handle；那次 `delete` 与本进程的注册表无关，登记与 `<db>/default` 视图都保留。
- **easy：`db->DropColumnFamily(h)`** → `MaybeForgetCF` → `UnRegisterColumnFamily` 按名字
  找到 registry clone → 摘除并删除；`h` 仍归调用者销毁，那次 destroy 与注册表无关。
- **插件：`repo.OpenDB`** → `MultiCF_Open`（默认 ctor）直接持有 `DB::Open` 的 handles，
  close 时由 repo 删。
- **插件：`dbm->CreateColumnFamily`** → 登记 clone，原样返回 core handle（用户的，用完自己
  destroy；`sample/dyna_new_cf.cc` 里就是这么写的）。drop 必须用 `dbm->DropColumnFamily`，
  不能用 core `db->DropColumnFamily`。
- **调用方**：`CloseAllDB(false)` 之后 repo 的 cf handle 已经被删除（`del_rocksdb_objs`
  只管 DB 对象），调用方不得再 `delete` 它们；工具里靠 `g_dbm`/`FLAGS_json` 区分
  （`tools/ldb_cmd.cc`、`tools/db_bench_tool.cc`、`db/c_test.c`）。
- **Java**：`CreateJDB` 返回 clone；`nativeCreateCF*` 返回的 handle 本来就归 Java。
  Java 必须自己 `close()` 这些 handle（`createCF` 的不在 `ownedColumnFamilyHandles` 里，
  `db.close()` 不会兜底），且 repo 关库之后 `db.close()` 删的是 DB 本身。顺序见
  `SidePluginRepo.java`：`nativeCloseAllDB()` → 逐个 `db.close()`。

## 4. 没做的事（都指向既有代码，与本次缺陷无关）

- 插件路径（`MultiCF_Open`）写 `props` 也不持 `db_mtx`；那是插件世界，本次未覆盖，保持原样。
- **契约**：从 repo create 的 cf，必须用 repo 的 `DropColumnFamily` 去 drop，不能用 core
  `db->DropColumnFamily(cfh)`。后者只有在 easy migrate 下才有 `MaybeForgetCF` 兜底；插件
  路径没有任何钩子，注册表会留下一个已经 drop 掉的 CF 的 handle（`get_cf_handles_view` 里
  也还有它）。这条没有运行时检查，靠约定。

## 5. 验证

| 用例 | 结果 |
|---|---|
| `db/db_cfh_ownership_test`（easy 7 例 + repo 世界 2 例，含 `CloseOneTrue`、`UserDestroyAfterClose`、`ConcurrentOpenClose`、`RepoPutPlainDB`） | 通过（9/9） |
| `/tmp/cfh_ownership_test`（早期版本） | 通过 |
| `/tmp/tdb_like_test`（create → destroy → close → 重开读） | 通过 |
| `sideplugin/rockside/test/cfh_close_test`（C API 3 例：并发、串行、handle 别名） | 通过（3/3） |
| `sample/getset`、`sample/dyna_new_cf` | 通过 |
| `CfhJavaTest`、`CfhJavaCycleTest`（`-ea -Xcheck:jni`，插件路径） | 通过 |
| `CfhJavaCoreTest`（只用 core API：open / createColumnFamily / dropColumnFamily / 关 handle / close，4 轮后重开校验） | easy migrate 关、开两种模式都通过 |
| `c_test`（`CARGO_SIDE_PLUGIN_OPEN_CF=1`，`side_plugin_repo_open` 的 CF 路径） | 通过 |
| `ldb`（`TOPLING_SIDEPLUGIN_CONF=...`，`scan`） | 通过 |
| `db_bench --json=...`：`fillseq,readseq` 与 `fillseq,fillrandom`（后者走 fresh_db 关库重开） | 通过 |

`close_one_true`（`CloseOneDB(db, true)`）在 `db_mtx` 还不是递归锁时 rc=124：
`~DBImpl → MaybeForgetDB` 去拿已被本线程持有的 `db_mtx`。换成递归锁后不再自锁；重入的
那次 `CloseOneDB` 则由「删 DB 前先注销」变成查不到、直接返回。

`CfhJavaCycleTest` 曾因用例没关 `repo.createCF()` 的 clone，在 dbg 构建下于
`~ColumnFamilySet()` 的 `assert(last_ref)` 处 abort；补上 `cfh.close()` 后 6 轮全过。
另外它的 DB 路径写死 `/dev/shm/cfh_plugin_db`，只有配 `cfh_plugin.yaml` 才对得上；
配错 conf 或在同一目录留了上一轮的 CF，会在 dbg 构建下以
`terminate called after throwing an instance of 'rocksdb::Status'` 直接 abort
（`JS_DB_MultiCF_Open` 里的 `throw s`，即 `DB::Open` 返回非 OK）。

Java 用例的 classpath 要用 `java/src/main/java` 现编出来的 classes，不要用
`java/target/rocksdbjni-*.jar`：仓库里这个 jar 的 class 可能是别的构建留下的
（`SidePluginRepo` 里调用的是当前源码里没有的 `makeRepoColumnFamilyHandle`），
用它跑会在第 0 轮 `closeAllDB()` 时撞 `last_ref`。
