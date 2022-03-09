# rockside 是 [ToplingDB](https://github.com/topling/toplingdb) 的核心 submodule

rockside 就是 [ToplingDB](https://github.com/topling/toplingdb) 的 [SidePlugin](https://github.com/topling/toplingdb/wiki) 插件体系的实现。

# ToplingDB
[ToplingDB](https://github.com/topling/toplingdb) fork 自 RocksDB，核心是两个 github 仓库：

github 仓库 | 内容
--------------------------------------------------|------------
[ToplingDB](https://github.com/topling/toplingdb) | fork 自 RocksDB，仅对 RocksDB 进行最少的必要的修改
[rockside](https://github.com/topling/rockside)   | [SidePlugin 插件体系](https://github.com/topling/toplingdb/wiki)，是 [ToplingDB](https://github.com/topling/toplingdb) 的 submodule

[ToplingDB](https://github.com/topling/toplingdb) 对 RocksDB 做了很多改进：

1. 修复了 RocksDB 的很多 Bug，其中有几十个修复已经 [Pull Request 到了上游 RocksDB](https://github.com/facebook/rocksdb/pulls?q=is%3Apr+author%3Arockeet)
2. [SidePlugin 插件体系](https://github.com/topling/toplingdb/wiki) 可以通过 json/yaml 设定 DB 的各种配置参数，用户代码只需要关注自己的业务逻辑，不需要操心任何配置相关的事情
3. SidePlugin 内置了对 RocksDB 自身所有组件的支持（例如各种 TableFactory, Cache, RateLimiter, WriteBufferManager 等等）
4. 通过 SidePlugin 框架，内嵌了一个 Http Web Server，可以展示 DB 的各种内部状态（例如展示当前生效的配置参数，展示 LSM 树形态等等），还可以将 DB 的各种内部指标导出到 Prometheus+grafana 实现监控
5. 通过内嵌的 Http Web Server，不用重启进程，[在线修改 DB 的各种配置参数](https://github.com/topling/rockside/wiki/Online-Change-Options)
6. [分布式 Compact](https://github.com/topling/rockside/wiki/Distributed-Compaction) 将 Compact 转移到专有的计算集群，并且这也是通过 json/yaml 配置来实现的，用户代码不需要为此进行任何修改！
7. [分布式 Compact](https://github.com/topling/rockside/wiki/Distributed-Compaction) 完美支持**带状态**的 CompactionFilter, MergeOperator, EventHandler 等等，当然，这需要用户自己实现**状态**的序列化/反序列化
8. Topling 性能组件：[更快的 MemTable](https://github.com/topling/rockside/wiki/ToplingCSPPMemTab)，[更快的SST](https://github.com/topling/rockside/wiki/ToplingFastTable)，[内存压缩的SST](https://github.com/topling/rockside/wiki/ToplingZipTable)，这些也是通过 json/yaml 来配置的，不需要修改用户代码
