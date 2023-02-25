//
// Created by leipeng on 2020/7/28.
//
#include "rocksdb/env.h"
#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "options/db_options.h"
#include "rocksdb/compaction_filter.h"
#include "rocksdb/concurrent_task_limiter.h"
#include "rocksdb/flush_block_policy.h"
#include "rocksdb/merge_operator.h"
#include "rocksdb/rate_limiter.h"
#include "rocksdb/slice_transform.h"
#include "rocksdb/sst_file_manager.h"
#include "rocksdb/wal_filter.h"
#include "json.h"
#include "side_plugin_factory.h"
#include "side_plugin_internal.h"

namespace ROCKSDB_NAMESPACE {

using std::shared_ptr;
using std::vector;
using std::string;

/*
{
  "DB::Open": {
    "cf_options" : {
      "default": {},
      "user-cf1": {}
    },
    "db_options" : {

    }
  },
  "db": "DB::Open"
}

*/

DB_MultiCF_Impl::DB_MultiCF_Impl() {
  m_catch_up_running = false;
  m_catch_up_delay_ms = 10;
}
DB_MultiCF_Impl::~DB_MultiCF_Impl() {
  ROCKSDB_VERIFY_F(nullptr == m_catch_up_thread,
                   "not closed: dbname = %s", db->GetName().c_str());
}

ColumnFamilyHandle* DB_MultiCF_Impl::Get(const std::string& cfname) const {
  auto iter = m_cfhs.name2p->find(cfname);
  if (m_cfhs.name2p->end() != iter) {
    return iter->second;
  }
  return nullptr;
}

Status DB_MultiCF_Impl::CreateColumnFamily(const std::string& cfname,
                                           const std::string& json_str,
                                           ColumnFamilyHandle** cfh)
try {
  auto js = json::parse(json_str);
  auto cfo = ObtainOPT(m_repo->m_impl->cf_options, "CFOptions", js, *m_repo);
  if (!m_create_cf) {
    return Status::InvalidArgument(ROCKSDB_FUNC, "DataBase is read only");
  }
  *cfh = m_create_cf(db, cfname, *cfo, js);
  cf_handles.push_back(*cfh);
  return Status::OK();
}
catch (const std::exception& ex) {
  return Status::InvalidArgument(ROCKSDB_FUNC, ex.what());
}
catch (const Status& st) {
  return st;
}

Status DB_MultiCF_Impl::DropColumnFamily(const std::string& cfname) try {
  auto iter = m_cfhs.name2p->find(cfname);
  if (m_cfhs.name2p->end() == iter) {
    return Status::NotFound(ROCKSDB_FUNC, "cfname = " + cfname);
  }
  ColumnFamilyHandle* cfh = iter->second;
  m_cfhs.name2p->erase(iter);
  m_cfhs.p2name.erase(cfh);
  db->DropColumnFamily(cfh);
  db->DestroyColumnFamilyHandle(cfh);
  return Status::OK();
}
catch (const std::exception& ex) {
  return Status::InvalidArgument(ROCKSDB_FUNC, ex.what());
}
catch (const Status& st) {
  return st;
}

Status DB_MultiCF_Impl::DropColumnFamily(ColumnFamilyHandle* cfh) {
  return DropColumnFamily(cfh->GetName());
}

void DB_MultiCF_Impl::AddOneCF_ToMap(const std::string& cfname,
                                     ColumnFamilyHandle* cfh,
                                     const json& js) {
  m_cfhs.name2p->emplace(cfname, cfh);
  m_cfhs.p2name.emplace(cfh, SidePluginRepo::Impl::ObjInfo{cfname, js});
}

void DB_MultiCF_Impl::InitAddCF_ToMap(const json& js_cf_desc) {
  size_t i = 0;
  assert(js_cf_desc.size() == cf_handles.size());
  for (auto& item : js_cf_desc.items()) {
    auto& cfname = item.key();
    auto& js_cf = item.value();
    ColumnFamilyHandle* cfh = cf_handles[i];
    assert(cfname == cfh->GetName());
    AddOneCF_ToMap(cfname, cfh, js_cf);
    i++;
  }
}

Status MergeTables(const std::vector<std::string>& files, const std::string& dbname,
                   const DBOptions& dbo, std::vector<ColumnFamilyDescriptor> cfo,
                   std::vector<std::string>* output) {
  // compact all files once?
  cfo[0].options.compaction_style = kCompactionStyleUniversal;
  cfo[0].options.compaction_options_universal = CompactionOptionsUniversal();
  cfo[0].options.compaction_options_universal.max_merge_width = 30;
  cfo[0].options.disable_auto_compactions = true;
  DB* db = nullptr;
  std::vector<ColumnFamilyHandle*> cfh;
  Status s = DB::Open(dbo, dbname, cfo, &cfh, &db);
  if (!s.ok()) return s;
  std::vector<rocksdb::IngestExternalFileArg> args(1);
  args[0].options.move_files = true;
  args[0].options.snapshot_consistency = false;
  args[0].options.allow_blocking_flush = false;
  args[0].options.allow_global_seqno = true;
  args[0].options.write_global_seqno = false;
  args[0].column_family = cfh[0];
  args[0].external_files = files;
  s = db->IngestExternalFiles(args);
  if (!s.ok()) return s;
  CompactRangeOptions cro;
  cro.target_level = cfo[0].options.num_levels - 1;
  s = db->CompactRange(cro, cfh[0], nullptr, nullptr);
  if (!s.ok()) return s;
  uint64_t manifest_file_size = 0;
  s = db->GetLiveFiles(*output, &manifest_file_size);
  auto is_not_sst = [](const Slice fname) {
    return !fname.ends_with(".sst");
  };
  auto last_sst = std::remove_if(output->begin(), output->end(), is_not_sst);
  output->erase(last_sst, output->end());
  for (auto& fname : *output) {
    fname = dbname + fname;
  }
  db->DestroyColumnFamilyHandle(cfh[0]).PermitUncheckedError();
  delete db;
  return s;
}

} // namespace ROCKSDB_NAMESPACE
