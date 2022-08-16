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

#include "side_plugin_factory.h"
#include "side_plugin_internal.h"

namespace ROCKSDB_NAMESPACE {

using std::shared_ptr;
using std::vector;
using std::string;

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

} // ROCKSDB_NAMESPACE
