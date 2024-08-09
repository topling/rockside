//
// Created by leipeng on 2020/7/12.
//

#include <memory>
#include <cinttypes>
#include <chrono>
#include <sstream>
#include <array>
#include <algorithm>
#include <bitset>

#include <rocksdb/db.h>
#include <rocksdb/sst_file_manager.h>
#include <port/likely.h>
#include <env/composite_env_wrapper.h>
#include <env/env_chroot.h>
#include <env/fs_cat.h>

#include <logging/auto_roll_logger.h>
#include <utilities/merge_operators/bytesxor.h>
#include <utilities/merge_operators/sortlist.h>
#include <utilities/merge_operators/string_append/stringappend2.h>

#include "side_plugin_factory.h"
#include "side_plugin_internal.h"

namespace ROCKSDB_NAMESPACE {

using std::shared_ptr;
using std::vector;
using std::string;

static Env* JS_NewChrootEnv(const json& js, const SidePluginRepo& repo) {
  Env* base_env = nullptr;
  std::string chroot_dir;
  ROCKSDB_JSON_REQ_PROP(js, chroot_dir);
  ROCKSDB_JSON_OPT_FACT(js, base_env);
  if (!base_env) {
    THROW_InvalidArgument("param 'base_env' is required");
  }
  return NewChrootEnv(base_env, chroot_dir);
}
ROCKSDB_FACTORY_REG("ChrootEnv", JS_NewChrootEnv);

static std::shared_ptr<FileSystem>
JS_NewChrootFileSystem(const json& js, const SidePluginRepo& repo) {
  std::shared_ptr<FileSystem> base_fs;
  std::string chroot_dir;
  ROCKSDB_JSON_REQ_PROP(js, chroot_dir);
  ROCKSDB_JSON_OPT_FACT(js, base_fs);
  if (!base_fs) {
    THROW_InvalidArgument("param 'base' is required");
  }
  return NewChrootFileSystem(base_fs, chroot_dir);
}
ROCKSDB_FACTORY_REG("ChrootFileSystem", JS_NewChrootFileSystem);

// never free the returned env
static Env* JS_NewCompositeEnv(const json& js, const SidePluginRepo& repo) {
  Env* base_env = Env::Default();
  std::shared_ptr<FileSystem> file_system = base_env->GetFileSystem();
  std::shared_ptr<SystemClock> system_clock = base_env->GetSystemClock();
  ROCKSDB_JSON_OPT_FACT(js, base_env);
  ROCKSDB_JSON_OPT_FACT(js, file_system);
  // ROCKSDB_JSON_OPT_FACT(js, system_clock); // always default system_clock
  return new CompositeEnvWrapper(base_env, file_system, system_clock);
}
ROCKSDB_FACTORY_REG("CompositeEnv", JS_NewCompositeEnv);

static std::shared_ptr<FileSystem>
JS_NewCatFileSystem(const json& js, const SidePluginRepo& repo) {
  bool use_mmap_read = false;
  std::shared_ptr<FileSystem> local, remote;
  ROCKSDB_JSON_OPT_PROP(js, use_mmap_read);
  ROCKSDB_JSON_OPT_FACT(js, local);
  ROCKSDB_JSON_OPT_FACT(js, remote);
  if (!local) {
    THROW_InvalidArgument("param 'local' is required");
  }
  if (!remote) {
    THROW_InvalidArgument("param 'remote' is required");
  }
  return std::make_shared<CatFileSystem>(local, remote, use_mmap_read);
}
ROCKSDB_FACTORY_REG("CatFileSystem", JS_NewCatFileSystem);

// lazy create logger to kill dependency from logger to DBOptions,
// this is not needed on most all cases, just for some corner use case.
class AntiDependsLogger : public Logger {
  std::string dbopt_varname;
  std::string dbhome;
  mutable std::shared_ptr<Logger> m_target;
  mutable std::mutex m_mtx;
  const SidePluginRepo* m_repo;
  Logger* Target() const {
    if (LIKELY(m_target != nullptr)) {
      return m_target.get();
    }
    std::lock_guard<std::mutex> lock(m_mtx);
    if (m_target != nullptr) {
      return m_target.get();
    }
    const DBOptions* dbo = nullptr;
    {
      auto name2p = m_repo->m_impl->db_options.name2p.get();
      auto iter = name2p->find(dbopt_varname);
      if (name2p->end() == iter) {
        THROW_InvalidArgument("can not find dbopt name = " + dbopt_varname);
      }
      dbo = iter->second.get();
    }
    Status s = CreateLoggerFromOptions(dbhome, *dbo, &m_target);
    if (!s.ok()) {
      throw s;
    }
    return m_target.get();
  }
public:
  AntiDependsLogger(const json& js, const SidePluginRepo& repo) {
    m_repo = &repo;
    ROCKSDB_JSON_REQ_PROP(js, dbopt_varname);
    ROCKSDB_JSON_REQ_PROP(js, dbhome); // dbname(default db home dir)
  }
  void LogHeader(const char* format, va_list ap) override {
    Target()->LogHeader(format, ap);
  }
  void Logv(const char* format, va_list ap) override {
    Target()->Logv(format, ap);
  }

  void Logv(const InfoLogLevel log_level, const char* format, va_list ap)
  override {
    Target()->Logv(log_level, format, ap);
  }
  size_t GetLogFileSize() const override {
    return Target()->GetLogFileSize();
  }
  void Flush() override { Target()->Flush(); }
/*
  InfoLogLevel GetInfoLogLevel() const {
    //ROCKSDB_ASSERT_EQ(Logger::GetInfoLogLevel(), Target()->GetInfoLogLevel());
    return Logger::GetInfoLogLevel();
  }
*/
  void SetInfoLogLevel(const InfoLogLevel log_level) override {
    Logger::SetInfoLogLevel(log_level);
    Target()->SetInfoLogLevel(log_level);
  }

 protected:
  Status CloseImpl() override {
    closed_ = true;
    if (m_target) m_target->Close();
    return Status::OK();
  }
};
ROCKSDB_REG_Plugin(AntiDependsLogger, Logger);
ROCKSDB_REG_Plugin("CreateLoggerFromOptions", AntiDependsLogger, Logger);

static std::shared_ptr<Logger>
JS_NewAutoRollLogger(const json& js, const SidePluginRepo& repo) {
  std::shared_ptr<FileSystem> fs;
  std::shared_ptr<SystemClock> clock = Env::Default()->GetSystemClock();
  std::string dbname;
  std::string db_log_dir;
  size_t log_max_size = 0;
  size_t log_file_time_to_roll = 0;
  size_t keep_log_file_num = 1000; // same with DBOptions default
  InfoLogLevel log_level = INFO_LEVEL;
  ROCKSDB_JSON_OPT_FACT(js, fs);
//ROCKSDB_JSON_OPT_FACT(js, clock); // not factoy
  ROCKSDB_JSON_REQ_PROP(js, dbname);
  ROCKSDB_JSON_OPT_PROP(js, db_log_dir);
  ROCKSDB_JSON_OPT_SIZE(js, log_max_size);
  ROCKSDB_JSON_OPT_PROP(js, log_file_time_to_roll);
  ROCKSDB_JSON_OPT_PROP(js, keep_log_file_num);
  ROCKSDB_JSON_OPT_ENUM(js, log_level);
  if (!fs) {
    fs = Env::Default()->GetFileSystem();
  }
  return std::make_shared<AutoRollLogger>(fs, clock, dbname, db_log_dir,
           log_max_size, log_file_time_to_roll, keep_log_file_num, log_level);
}
ROCKSDB_FACTORY_REG("AutoRollLogger", JS_NewAutoRollLogger);

static std::shared_ptr<SstFileManager>
JS_NewSstFileManager(const json& js, const SidePluginRepo& repo) {
  Env* env = Env::Default();
  std::shared_ptr<FileSystem> fs = nullptr;
  std::shared_ptr<Logger> info_log = nullptr;
  std::string trash_dir; // default empty
  int64_t rate_bytes_per_sec = 0;
  bool delete_existing_trash = true;
  Status status;
  double max_trash_db_ratio = 0.25;
  uint64_t bytes_max_delete_chunk = 64 * 1024 * 1024;
  ROCKSDB_JSON_OPT_FACT(js, env);
  ROCKSDB_JSON_OPT_FACT(js, fs);
  ROCKSDB_JSON_OPT_FACT(js, info_log);
  ROCKSDB_JSON_OPT_PROP(js, trash_dir);
  ROCKSDB_JSON_OPT_SIZE(js, rate_bytes_per_sec);
  ROCKSDB_JSON_OPT_PROP(js, max_trash_db_ratio);
  ROCKSDB_JSON_OPT_SIZE(js, bytes_max_delete_chunk);
  if (!fs) {
    fs = env->GetFileSystem();
  }
  SstFileManager* mgr = NewSstFileManager(env, fs, info_log,
      trash_dir, rate_bytes_per_sec, delete_existing_trash, &status,
      max_trash_db_ratio, bytes_max_delete_chunk);
  if (!status.ok()) {
    throw status;
  }
  return std::shared_ptr<SstFileManager>(mgr);
}
ROCKSDB_FACTORY_REG("SstFileManager", JS_NewSstFileManager);
ROCKSDB_FACTORY_REG("Default", JS_NewSstFileManager);

/////////////////////////////////////////////////////////////////////////////

ROCKSDB_REG_Plugin(BytesXOROperator, MergeOperator);
ROCKSDB_REG_Plugin("BytesXOR", BytesXOROperator, MergeOperator);
ROCKSDB_REG_Plugin("bytesxor", BytesXOROperator, MergeOperator);

ROCKSDB_REG_Plugin("sortlist", SortList, MergeOperator);
ROCKSDB_REG_Plugin("MergeSortOperator", SortList, MergeOperator);

static std::shared_ptr<MergeOperator>
JS_CreateUInt64AddOperator(const json&, const SidePluginRepo&) {
  return MergeOperators::CreateUInt64AddOperator();
}
ROCKSDB_FACTORY_REG("UInt64AddOperator", JS_CreateUInt64AddOperator);
ROCKSDB_FACTORY_REG("uint64add", JS_CreateUInt64AddOperator);

static std::shared_ptr<MergeOperator>
JS_NewStringAppendMergeOperator(const json& js, const SidePluginRepo&) {
  std::string delim;
  ROCKSDB_JSON_OPT_PROP(js, delim);
  // StringAppendTESTOperator implements MergeOperator V2
  return std::make_shared<StringAppendTESTOperator>(delim);
}
ROCKSDB_FACTORY_REG("StringAppendTESTOperator", JS_NewStringAppendMergeOperator);
ROCKSDB_FACTORY_REG("stringappendtest", JS_NewStringAppendMergeOperator);

} // ROCKSDB_NAMESPACE
