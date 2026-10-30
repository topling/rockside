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
#include "db/column_family.h"
#include "json.h"
#include "side_plugin_factory.h"
#include "side_plugin_internal.h"

namespace ROCKSDB_NAMESPACE {

using std::shared_ptr;
using std::vector;
using std::string;

DB_MultiCF_Impl::DB_MultiCF_Impl() {
  m_catch_up_running = false;
  m_catch_up_delay_ms = 10;
}
DB_MultiCF_Impl::~DB_MultiCF_Impl() {
  ROCKSDB_VERIFY_F(nullptr == m_catch_up_thread,
                   "not closed: dbname = %s", db->GetName().c_str());
}

ColumnFamilyHandle* DB_MultiCF_Impl::Get(const std::string& cfname) const {
  m_mtx.lock_shared();
  ROCKSDB_SCOPE_EXIT(m_mtx.unlock());
  auto iter = m_cfhs.name2p->find(cfname);
  if (m_cfhs.name2p->end() != iter) {
    return iter->second;
  }
  return nullptr;
}

extern // defined in builtin_pluin_misc.cc
void AddCFPropertiesWebView(DB_MultiCF*, ColumnFamilyHandle*,
                            const std::string& cfname,
                            const SidePluginRepo& repo);

extern // defined in builtin_plugin_misc.cc
void RemoveCFPropertiesWebView(DB_MultiCF*, const std::string& cfname,
                               const SidePluginRepo& repo);

json JsonFromText(const std::string& text) {
  const char* beg = text.data();
  const char* end = text.data() + text.size();
  while (beg < end && isspace((unsigned char)(*beg))) {
    beg++;
  }
  while (beg < end && isspace((unsigned char)(end[-1]))) {
    end--;
  }
  if (beg == end) {
    //THROW_InvalidArgument("json text is empty or all spaces");
    return json(std::string()); // empty string
  }
  if (beg[0] == '{' || beg[0] == '[' || beg[0] == '"') {
    // object or array or string
    return json::parse(beg, end);
  }
  else {
    if (end - beg >= 2 && beg[0] == '\'' && end[-1] == '\'') {
      beg++;
      end--;
    }
    return json(std::string(beg, end)); // string
  }
}

Status DB_MultiCF_Impl::CreateColumnFamily(const std::string& cfname,
                                           const std::string& json_str,
                                           ColumnFamilyHandle** cfh)
try {
  json js = JsonFromText(json_str);
  auto cfo = ObtainOPT(m_repo->m_impl->cf_options, "CFOptions", js, *m_repo);
  if (!m_create_cf) {
    return Status::InvalidArgument(ROCKSDB_FUNC, "DataBase is read only");
  }
  auto cfh1 = m_create_cf(db, cfname, *cfo, js);
//*cfh = cfh1->CloneHandle(); // return a clone
  *cfh = cfh1;
  {
    std::lock_guard<std::shared_mutex> lk(m_mtx);
    AddOneCF_ToMap(cfname, cfh1, js);
    cf_handles.push_back(cfh1);
  }
  AddCFPropertiesWebView(this, cfh1, cfname, *m_repo);
  return Status::OK();
}
catch (const std::exception& ex) {
  return Status::InvalidArgument(ROCKSDB_FUNC, ex.what());
}
catch (const Status& st) {
  return st;
}

Status DB_MultiCF_Impl::CreateColumnFamilyWithImport(
            const std::string& cfname,
            const ImportColumnFamilyOptions& import_options,
            const std::vector<const ExportImportFilesMetaData*>& metadatas,
            const std::string& json_str, ColumnFamilyHandle** cfh)
try {
  json js = JsonFromText(json_str);
  auto cfo = ObtainOPT(m_repo->m_impl->cf_options, "CFOptions", js, *m_repo);
  if (!m_create_cf) {
    return Status::InvalidArgument(ROCKSDB_FUNC, "DataBase is read only");
  }
  auto cfh1 = m_create_cf_with_import(db, cfname, *cfo, import_options, metadatas, js);
  //*cfh = cfh1->CloneHandle(); // return a clone
  *cfh = cfh1;
  {
    std::lock_guard<std::shared_mutex> lk(m_mtx);
    AddOneCF_ToMap(cfname, cfh1, js);
    cf_handles.push_back(cfh1);
  }
  AddCFPropertiesWebView(this, cfh1, cfname, *m_repo);
  return Status::OK();
}
catch (const std::exception& ex) {
  return Status::InvalidArgument(ROCKSDB_FUNC, ex.what());
}
catch (const Status& st) {
  return st;
}

Status DB_MultiCF_Impl::DropColumnFamily(const std::string& cfname)
try {
  ColumnFamilyHandle* cfh = nullptr;
  {
    std::lock_guard<std::shared_mutex> write_lock(m_mtx);
    auto iter = m_cfhs.name2p->find(cfname);
    if (m_cfhs.name2p->end() == iter) {
      return Status::NotFound(ROCKSDB_FUNC, "cfname = " + cfname);
    }
    cfh = iter->second;
    m_cfhs.name2p->erase(iter);
    m_cfhs.p2name.erase(cfh);
    auto rm_iter = std::remove(cf_handles.begin(), cf_handles.end(), cfh);
    ROCKSDB_VERIFY(rm_iter + 1 == cf_handles.end());
    cf_handles.erase(rm_iter, cf_handles.end());
  }
  Status s = db->DropColumnFamily(cfh);
  if (!s.ok()) {
    fprintf(stderr, "ERROR: DropColumnFamily(%s): %s\n",
            cfname.c_str(), s.ToString().c_str());
  }
  RemoveCFPropertiesWebView(this, cfname, *m_repo);
  db->DestroyColumnFamilyHandle(cfh); // check and delete
  return Status::OK();
}
catch (const std::exception& ex) {
  return Status::InvalidArgument(ROCKSDB_FUNC, ex.what());
}
catch (const Status& st) {
  return st;
}

///@param cfh: the CF to drop; only the registry's handle is destroyed
Status DB_MultiCF_Impl::DropColumnFamily(ColumnFamilyHandle* cfh) {
  return DropColumnFamily(cfh->GetName());
}

void DB_MultiCF_Impl::DoRegisterColumnFamily(ColumnFamilyHandle* cfh) {
  // Used for easy migrate, never del cfh in SidePluginRepo
  TERARK_VERIFY_EQ(GetEasyMigrateSidePluginRepo().get(), m_repo);
  const std::string& cfname = cfh->GetName();
  {
    std::lock_guard<std::shared_mutex> lk(m_mtx);
    AddOneCF_ToMap(cfname, cfh, {});
    cf_handles.push_back(cfh);
  }
  AddCFPropertiesWebView(this, cfh, cfname, *m_repo);
}

void DB_MultiCF_Impl::UnRegisterColumnFamily(ColumnFamilyHandle* cfh) {
  // Used for easy migrate, never del cfh in SidePluginRepo
  // cfh is the caller's, the handle deleted below is the registry's clone
  TERARK_VERIFY_EQ(GetEasyMigrateSidePluginRepo().get(), m_repo);
  const std::string& cfname = cfh->GetName();
  RemoveCFPropertiesWebView(this, cfname, *m_repo);
  {
    std::lock_guard<std::shared_mutex> write_lock(m_mtx);
    auto iter = m_cfhs.name2p->find(cfname);
    if (m_cfhs.name2p->end() == iter) {
      // DB_MultiCF_Impl::DropColumnFamily() removes the entry before calling
      // the core drop, which comes back here through MaybeForgetCF(); a dropped
      // CF is the normal sequence, so only warn if it is still alive.
      if (!cfh->cfd()->IsDropped()) {
        fprintf(stderr, "WARN: UnRegisterColumnFamily: not found cfname = %s\n", cfname.c_str());
      }
      return;
    }
    cfh = iter->second; // repo's cfh
    m_cfhs.name2p->erase(iter);
    m_cfhs.p2name.erase(cfh);
    auto rm_iter = std::remove_if(cf_handles.begin(), cf_handles.end(),
    [cfd = cfh->cfd()](ColumnFamilyHandle* h) {
      return cfd == h->cfd(); // handle maybe clone, cfd is unique
    });
    ROCKSDB_VERIFY(rm_iter + 1 == cf_handles.end());
    cf_handles.erase(rm_iter, cf_handles.end());
  }
  delete cfh;
}

std::vector<ColumnFamilyHandle*> DB_MultiCF_Impl::get_cf_handles_view() const {
  m_mtx.lock_shared();
  ROCKSDB_SCOPE_EXIT(m_mtx.unlock());
  return cf_handles;
}

void DB_MultiCF_Impl::AddOneCF_ToMap(const std::string& cfname,
                                     ColumnFamilyHandle* cfh,
                                     const json& js) {
  m_cfhs.name2p->emplace(cfname, cfh);
  m_cfhs.p2name.emplace(cfh, SidePluginRepo::Impl::ObjInfo{cfname, js});
}

void DB_MultiCF_Impl::InitAddCF_ToMap(const json& js_cf_desc) {
  size_t i = 0;
  ROCKSDB_VERIFY_EQ(js_cf_desc.size(), cf_handles.size());
  for (auto& item : js_cf_desc.items()) {
    auto& cfname = item.key();
    auto& js_cf = item.value();
    ColumnFamilyHandle* cfh = cf_handles[i];
    TERARK_VERIFY_S_EQ(cfname, cfh->GetName());
    AddOneCF_ToMap(cfname, cfh, js_cf);
    i++;
  }
}

void DB_UpdateMaxColumnFamily(DB* db, uint32_t max_cf_id);
Status MergeTables(const std::vector<std::string>& files, const std::string& dbname,
                   const DBOptions& dbo, std::vector<ColumnFamilyDescriptor> cfo,
                   uint32_t cf_id,
                   std::vector<std::string>* output) {
  // compact all files once?
  cfo[0].options.compaction_style = kCompactionStyleUniversal;
  cfo[0].options.compaction_options_universal = CompactionOptionsUniversal();
  cfo[0].options.compaction_options_universal.max_merge_width = 30;
  cfo[0].options.disable_auto_compactions = true;
  std::string target_cfname = cfo[0].name;
  cfo[0].name = "default";
  DB* db = nullptr;
  std::vector<ColumnFamilyHandle*> cfh;
  Status s = DB::Open(dbo, dbname, cfo, &cfh, &db);
  if (!s.ok()) return s;
  ColumnFamilyHandle* target_cfh = cfh[0];
  if (target_cfname != "default") {
    ROCKSDB_VERIFY_GE(cf_id, 1);
    DB_UpdateMaxColumnFamily(db, cf_id - 1);
    s = db->CreateColumnFamily(cfo[0].options, target_cfname, &target_cfh);
    TERARK_VERIFY_S(s.ok(), "CreateColumnFamily: %s", s.ToString());
    TERARK_VERIFY_EQ(target_cfh->GetID(), cf_id);
  }
  std::vector<rocksdb::IngestExternalFileArg> args(1);
  args[0].options.move_files = true;
  args[0].options.snapshot_consistency = false;
  args[0].options.allow_blocking_flush = false;
  args[0].options.allow_global_seqno = true;
  args[0].options.write_global_seqno = false;
  args[0].column_family = target_cfh;
  args[0].external_files = files;
  s = db->IngestExternalFiles(args);
  if (!s.ok()) return s;
  CompactRangeOptions cro;
  cro.target_level = cfo[0].options.num_levels - 1;
  s = db->CompactRange(cro, target_cfh, nullptr, nullptr);
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
  if (target_cfh != cfh[0]) {
    db->DestroyColumnFamilyHandle(target_cfh).PermitUncheckedError();
  }
  db->DestroyColumnFamilyHandle(cfh[0]).PermitUncheckedError();
  delete db;
  return s;
}

} // namespace ROCKSDB_NAMESPACE
