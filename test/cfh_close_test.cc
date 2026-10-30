//  Copyright (c) 2026-present, Topling Inc.
//  C API regression test for the Easy-Migrate bookkeeping at database close:
//
//    ROCKSDB_VERIFY_F(cfh_to_view.end() != iter, "cfh must in cfh_to_view")
//    sideplugin/rockside/src/topling/builtin_plugin_misc.cc, CloseOneDB()
//
//  The crash needs two databases opened and closed at the same time, not a
//  particular close order.  Easy Migrate registers a database from DB::Open():
//
//    DB::Open -> ... -> MaybeRetainDB -> SidePluginRepo::Put
//                  -> Impl_Put / Impl_PutTpl / SetCFPropertiesWebView
//
//  and that whole path used to take no lock, while CloseOneDB() takes db_mtx
//  and then iterates props.p2name to build the `cfh_to_view` snapshot.  A
//  concurrent insert in the unlock path can make that traversal skip a node --
//  and the skipped node is exactly the view the close later looks up, which is
//  the verify above.  A traversal can also meet a node twice, which makes
//  CloseOneDB's del_view() erase it twice and leaves the orphan key "".
//
//  The second half is the handle lifetime: DestroyColumnFamilyHandle() is a
//  plain `delete`, it never calls MaybeForgetCF, so a handle destroyed before
//  rocksdb_close() stays in DB_MultiCF_Impl::cf_handles and the close walks a
//  list of addresses that the allocator may already have handed to another
//  database.
//
//  The arms:
//
//    ConcurrentOpenClose           TDengine's close order -- destroy the
//                                  handles, then rocksdb_close(), across
//                                  several threads
//    ConcurrentOpenCloseSerialized the same, but every open/close call is
//                                  mutually exclusive, so the registration
//                                  path never runs against a close
//    HandleAliasAfterDestroy       single-threaded probe for the lifetime
//                                  defect: a handle destroyed before
//                                  rocksdb_close() stays in cf_handles
//
//  Every arm ends by checking that the registry holds no property view, so a
//  close that leaks or double-erases a view fails the test even when it does
//  not abort.
//
//  A "only concurrent opens, no handle destruction at all" arm existed while
//  this was a standalone program: it needs the caller's handles to outlive the
//  database, which RocksDB's debug assertions reject on purpose
//  (db/column_family.cc: ColumnFamilySet::~ColumnFamilySet() 'last_ref'), so
//  it cannot live in a debug test binary.

#include <rocksdb/c.h>

#include <sys/stat.h>

#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <fstream>
#include <functional>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include <topling/side_plugin_factory.h>
#include <topling/side_plugin_repo.h>

#include "file/file_util.h"
#include "port/stack_trace.h"
#include "test_util/testharness.h"

namespace ROCKSDB_NAMESPACE {
namespace {

const char* kCfName = "tsdb.blk";

// Only turns the mechanism on: none of the arms depends on which factories are
// configured, and the databases stay small.
const char* kEasyConf = R"({
  "DBOptions": {"default": {"create_if_missing": true,
                            "create_missing_column_families": true,
                            "max_background_jobs": 4}},
  "CFOptions": {"default": {"num_levels": 7,
                            "write_buffer_size": 8388608,
                            "max_write_buffer_number": 2}},
  "http": {"auto_start_http": false}
})";

// Easy Migrate caches TOPLINGDB_EASY_MIGRATE_CONF on first use, so the file
// has to exist before the first database is opened in this process.
class EasyMigrateEnv : public ::testing::Environment {
 public:
  void SetUp() override {
    const std::string conf = test::TmpDir() + "/cfh_close.easy-migrate.json";
    std::ofstream fout(conf, std::ios::trunc);
    fout << kEasyConf;
    fout.close();
    ::setenv("TOPLINGDB_EASY_MIGRATE_CONF", conf.c_str(), 1);
    // Opening one database from several threads must not race on its
    // OPTIONS file.
    ::setenv("ROCKSDB_KICK_OUT_OPTIONS_FILE", "1", 1);
  }
};

::testing::Environment* const kEasyMigrateEnv =
    ::testing::AddGlobalTestEnvironment(new EasyMigrateEnv);

size_t Views() {
  const auto& repo = GetEasyMigrateSidePluginRepo();
  return repo ? repo->m_impl->props.name2p->size() : 0;
}

struct Config {
  std::string root;
  int threads = 4;
  int rounds = 10;
  // Widens the window between "destroy the handles" and "rocksdb_close" so the
  // lifetime half does not depend on scheduler luck.  The jitter keeps the
  // workers out of lockstep: with a uniform gap every thread sits in its
  // window at the same time and none of them is opening a database (i.e.
  // handing out the freed handle addresses) while another one is inside its
  // window.
  int gap_us = 200;
  bool jitter = true;
  // Control arm: make every open/close call mutually exclusive, so an open
  // never runs against a close and only the lifetime window remains.
  bool serialize = false;
  bool verbose = false;
  int pool = 16;  // databases kept open to recycle handle slots
};

// Only with serialize: keeps every open/close pair off each other so that the
// registration path and CloseOneDB()'s snapshot never overlap.
std::mutex g_api_mu;

void Check(char** err, const char* what, const std::string& path) {
  if (*err != nullptr) {
    fprintf(stderr, "FATAL: %s %s: %s\n", what, path.c_str(), *err);
    rocksdb_free(*err);
    *err = nullptr;
    abort();
  }
}

rocksdb_options_t* MakeOptions() {
  rocksdb_options_t* opt = rocksdb_options_create();
  rocksdb_options_set_create_if_missing(opt, 1);
  rocksdb_options_set_create_missing_column_families(opt, 1);
  // Same as tdb: the payload is compressed before it reaches the engine.
  rocksdb_options_set_compression(opt, rocksdb_no_compression);
  return opt;
}

std::vector<std::string> ListCfs(rocksdb_options_t* opt,
                                 const std::string& path) {
  char* err = nullptr;
  size_t n = 0;
  char** names = rocksdb_list_column_families(opt, path.c_str(), &n, &err);
  Check(&err, "list column families", path);
  std::vector<std::string> out;
  out.reserve(n);
  for (size_t i = 0; i < n; i++) {
    if (names[i] != nullptr) {
      out.emplace_back(names[i]);
    }
  }
  rocksdb_list_column_families_destroy(names, n);
  return out;
}

bool DirExists(const std::string& path) {
  struct stat st;
  return stat(path.c_str(), &st) == 0 && S_ISDIR(st.st_mode);
}

// TDengine tsdbBlkRdbEnsureCf(): create "tsdb.blk" before the open that carries
// the column family list.  The handle returned by rocksdb_create_column_family()
// is destroyed while the database is still open -- the first place where the
// registry keeps a handle the caller frees.
void EnsureCf(const Config& c, rocksdb_options_t* opt, const std::string& path) {
  char* err = nullptr;

  // A fresh vnode has no blk.rdb yet; list_column_families on a missing
  // database is an error, so create it (and its default CF) first.
  if (!DirExists(path)) {
    rocksdb_t* db = rocksdb_open(opt, path.c_str(), &err);
    Check(&err, "create", path);
    rocksdb_close(db);
  }

  std::vector<std::string> names = ListCfs(opt, path);
  for (const auto& name : names) {
    if (name == kCfName) {
      return;
    }
  }

  rocksdb_t* db = rocksdb_open(opt, path.c_str(), &err);
  Check(&err, "open", path);

  rocksdb_column_family_handle_t* cfh =
      rocksdb_create_column_family(db, opt, kCfName, &err);
  if (cfh != nullptr) {
    if (c.verbose) {
      fprintf(stderr, "[ensure] %s: created %s handle=%p, destroying it\n",
              path.c_str(), kCfName, (void*)cfh);
    }
    rocksdb_column_family_handle_destroy(cfh);
  }
  rocksdb_close(db);
  Check(&err, "create column family", path);
}

struct OpenDb {
  rocksdb_t* db = nullptr;
  std::vector<rocksdb_column_family_handle_t*> handles;
  std::string path;
};

OpenDb OpenWithAllCfs(const Config& c, rocksdb_options_t* opt,
                      const std::string& path) {
  OpenDb out;
  out.path = path;

  std::vector<std::string> names = ListCfs(opt, path);
  std::vector<const char*> cnames;
  std::vector<const rocksdb_options_t*> copts;
  cnames.reserve(names.size());
  copts.reserve(names.size());
  for (const auto& name : names) {
    cnames.push_back(name.c_str());
    copts.push_back(opt);
  }
  out.handles.assign(names.size(), nullptr);

  char* err = nullptr;
  out.db = rocksdb_open_column_families(opt, path.c_str(), (int)names.size(),
                                        cnames.data(), copts.data(),
                                        out.handles.data(), &err);
  Check(&err, "open with column families", path);
  if (c.verbose) {
    fprintf(stderr, "[open] %s: %zu handles\n", path.c_str(), out.handles.size());
  }
  return out;
}

// TDengine tsdbBlkRdbCloseImpl(): handles first, database second.
void DestroyHandles(const Config& c, OpenDb& d) {
  for (auto* h : d.handles) {
    if (h != nullptr) {
      if (c.verbose) {
        fprintf(stderr, "[close] %s: destroy handle=%p\n", d.path.c_str(),
                (void*)h);
      }
      rocksdb_column_family_handle_destroy(h);
    }
  }
  d.handles.clear();
}

void SleepInWindow(const Config& c, unsigned* seed) {
  if (c.gap_us > 0) {
    unsigned r = rand_r(seed);
    int gap = c.jitter ? (int)(r % (unsigned)(2 * c.gap_us + 1)) : c.gap_us;
    std::this_thread::sleep_for(std::chrono::microseconds(gap));
  }
}

void CloseDb(OpenDb& d) {
  if (d.db != nullptr) {
    rocksdb_close(d.db);  // -> ~DBImpl -> MaybeForgetDB -> CloseOneDB
    d.db = nullptr;
  }
}

// A pool database is closed with TDengine's order too: its handles must not
// outlive it, or RocksDB's own refcount assertion fires before the registry is
// ever consulted.
void DestroyAndClose(const Config& c, OpenDb& d) {
  DestroyHandles(c, d);
  CloseDb(d);
}

void PutOne(OpenDb& d, int i) {
  if (d.db == nullptr || d.handles.empty()) {
    return;
  }
  // Last column family, so the default handle is not the only one exercised.
  auto* cfh = d.handles.back();
  char key[32];
  int klen = snprintf(key, sizeof(key), "k%08d", i);
  char* err = nullptr;
  rocksdb_writeoptions_t* wo = rocksdb_writeoptions_create();
  rocksdb_put_cf(d.db, wo, cfh, key, (size_t)klen, key, (size_t)klen, &err);
  rocksdb_writeoptions_destroy(wo);
  Check(&err, "put", d.path);
}

void RunWorker(const Config& c, int id) {
  rocksdb_options_t* opt = MakeOptions();
  const std::string path = c.root + "/db" + std::to_string(id);

  // A fresh vnode: create the database and its "tsdb.blk" once.
  {
    std::unique_lock<std::mutex> lk(g_api_mu, std::defer_lock);
    if (c.serialize) lk.lock();
    EnsureCf(c, opt, path);
  }

  unsigned seed = (unsigned)(id * 2654435761u + 12345u);
  for (int r = 0; r < c.rounds; r++) {
    OpenDb d;
    {
      std::unique_lock<std::mutex> lk(g_api_mu, std::defer_lock);
      if (c.serialize) lk.lock();
      d = OpenWithAllCfs(c, opt, path);
    }
    PutOne(d, r);
    {
      std::unique_lock<std::mutex> lk(g_api_mu, std::defer_lock);
      if (c.serialize) lk.lock();
      DestroyHandles(c, d);
    }
    SleepInWindow(c, &seed);
    {
      std::unique_lock<std::mutex> lk(g_api_mu, std::defer_lock);
      if (c.serialize) lk.lock();
      CloseDb(d);
    }
  }
  rocksdb_options_destroy(opt);
}

void RunConcurrent(const Config& c) {
  std::vector<std::thread> threads;
  threads.reserve(c.threads);
  for (int i = 0; i < c.threads; i++) {
    threads.emplace_back(RunWorker, std::cref(c), i);
  }
  for (auto& t : threads) {
    t.join();
  }
}

// Open a database with a fixed column family set, creating what is missing.
OpenDb OpenFixed(rocksdb_options_t* opt, const std::string& path) {
  OpenDb out;
  out.path = path;
  const char* names[2] = {"default", kCfName};
  const rocksdb_options_t* opts[2] = {opt, opt};
  out.handles.assign(2, nullptr);
  char* err = nullptr;
  out.db = rocksdb_open_column_families(opt, path.c_str(), 2, names, opts,
                                        out.handles.data(), &err);
  Check(&err, "open fixed", path);
  return out;
}

// Single-threaded probe for the lifetime half of the defect, with no concurrent
// map access at all:
//
//   open A -> destroy A's handles -> open B -> close A -> close B
//
// If the allocator hands one of A's freed handle addresses to B, then A's close
// walks a stale handle list whose entries now belong to B.  Whether that turns
// into the "cfh must in cfh_to_view" abort depends only on which of the two
// views with that handle address the repo's address-keyed lookup picks, so the
// loop keeps going until it hits.
void RunAlias(const Config& c) {
  rocksdb_options_t* opt = MakeOptions();
  const std::string pa = c.root + "/aliasA";
  EnsureCf(c, opt, pa);

  // Pool databases exist to hand out fresh handles; keeping several open means
  // the allocation that recycles one of A's freed slots stays alive long enough
  // for A's close to look it up.
  std::vector<std::string> pool_paths;
  for (int i = 0; i < c.pool; i++) {
    pool_paths.push_back(c.root + "/pool" + std::to_string(i));
  }

  for (int attempt = 0; attempt < c.rounds; attempt++) {
    OpenDb a = OpenFixed(opt, pa);
    std::vector<rocksdb_column_family_handle_t*> freed = a.handles;
    DestroyHandles(c, a);  // a.handles cleared, a.db still open

    std::vector<OpenDb> pool;
    for (const auto& path : pool_paths) {
      pool.push_back(OpenFixed(opt, path));
    }

    // Which pool handle, if any, landed on one of A's freed addresses?
    int victim = -1;
    rocksdb_column_family_handle_t* reused = nullptr;
    for (size_t i = 0; i < pool.size() && victim < 0; i++) {
      for (auto* hb : pool[i].handles) {
        for (auto* ha : freed) {
          if (ha == hb) {
            victim = (int)i;
            reused = hb;
          }
        }
      }
    }

    if (victim < 0) {
      if (c.verbose) {
        fprintf(stderr, "[alias] attempt %d: no reuse\n", attempt);
      }
      for (auto& p : pool) DestroyAndClose(c, p);
      CloseDb(a);
      continue;
    }

    fprintf(stderr,
            "[alias] attempt %d: A's freed handle %p is now live in pool[%d]\n",
            attempt, (void*)reused, victim);

    CloseDb(a);  // CloseOneDB() over A's stale handle list
    fprintf(stderr, "[alias] attempt %d: A closed, closing pool[%d]\n", attempt,
            victim);
    DestroyAndClose(c, pool[victim]);  // must find its own view in cfh_to_view
    fprintf(stderr, "[alias] attempt %d: victim closed, no abort\n", attempt);

    for (size_t i = 0; i < pool.size(); i++) {
      if ((int)i != victim) DestroyAndClose(c, pool[i]);
    }
  }
  rocksdb_options_destroy(opt);
}

class CfhCloseTest : public ::testing::Test {
 protected:
  void SetUp() override {
    // CreateDirIfMissing() makes one level only, so the root sits directly in
    // the temporary directory rather than in a subdirectory of it.
    config_.root = test::TmpDir() + "/cfh_close_" +
                   ::testing::UnitTest::GetInstance()->current_test_info()->name();
    DestroyDir(Env::Default(), config_.root).PermitUncheckedError();
    ASSERT_OK(Env::Default()->CreateDirIfMissing(config_.root));
  }

  Config config_;
};

TEST_F(CfhCloseTest, ConcurrentOpenClose) {
  RunConcurrent(config_);
  ASSERT_EQ(Views(), 0U);
}

TEST_F(CfhCloseTest, ConcurrentOpenCloseSerialized) {
  config_.serialize = true;
  RunConcurrent(config_);
  ASSERT_EQ(Views(), 0U);
}

TEST_F(CfhCloseTest, HandleAliasAfterDestroy) {
  RunAlias(config_);
  ASSERT_EQ(Views(), 0U);
}

}  // namespace
}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
