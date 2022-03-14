//
// Created by leipeng on 2020/7/1.
//
#include <cinttypes>
#include <fstream>
#include <sstream>

#include "rocksdb/env.h"
#include "rocksdb/options.h"
#include "options/db_options.h"
#include "rocksdb/compaction_filter.h"
#include "rocksdb/concurrent_task_limiter.h"
#include "rocksdb/flush_block_policy.h"
#include "rocksdb/merge_operator.h"
#include "rocksdb/rate_limiter.h"
#include "rocksdb/slice_transform.h"
#include "rocksdb/sst_file_manager.h"
#include "rocksdb/utilities/transaction_db_mutex.h"
#include "rocksdb/wal_filter.h"
#include "util/string_util.h"

#include "json.h"
#include "side_plugin_factory.h"

#if defined(__GNUC__)
# include <cxxabi.h>
#endif

namespace ROCKSDB_NAMESPACE {

using std::shared_ptr;
using std::vector;
using std::string;

/////////////////////////////////////////////////////////////////////////////
template<class Ptr> // just for type deduction
static Ptr RepoPtrType(const SidePluginRepo::Impl::ObjMap<Ptr>&);
template<class T> // just for type deduction
static const shared_ptr<T>&
RepoPtrCref(const SidePluginRepo::Impl::ObjMap<shared_ptr<T> >&);

template<class T> // just for type deduction
static T* RepoPtrCref(const SidePluginRepo::Impl::ObjMap<T*>&);

template<class T> // just for type deduction
static const T*
RepoConstRawPtr(const SidePluginRepo::Impl::ObjMap<shared_ptr<T> >&);

template<class T> // just for type deduction
static const T*
RepoConstRawPtr(const SidePluginRepo::Impl::ObjMap<T*>&);

std::string JsonGetClassName(const char* caller, const json& js) {
  if (js.is_string()) {
    return js.get_ref<const std::string&>();
  }
  if (js.is_object()) {
    auto iter = js.find("class");
    if (js.end() != iter) {
      if (!iter.value().is_string())
        throw Status::InvalidArgument(caller,
          "json[\"class\"] must be string, but is: " + js.dump());
      return iter.value().get_ref<const std::string&>();
    }
    throw Status::InvalidArgument(caller,
      "json missing sub obj \"class\": " + js.dump());
  }
  throw Status::InvalidArgument(caller,
    "json must be string or object, but is: " + js.dump());
}

template<class Ptr>
static void Impl_Import(SidePluginRepo::Impl::ObjMap<Ptr>& field,
                   const std::string& name,
                   const json& main_js, const SidePluginRepo& repo) {
  auto iter = main_js.find(name);
  if (main_js.end() == iter) {
      return;
  }
  if (!iter.value().is_object()) {
    THROW_InvalidArgument(name + " must be an object with class and options");
  }
  for (auto& item : iter.value().items()) {
    const string& inst_id = item.key();
    json value = item.value();
    auto ib = field.name2p->emplace(inst_id, Ptr(nullptr));
    auto& existing = ib.first->second;
    if (!ib.second) { // existed
      assert(Ptr(nullptr) != existing);
      auto oi_iter = field.p2name.find(GetRawPtr(existing));
      if (field.p2name.end() == oi_iter) {
        THROW_Corruption("p2name[ptr_of(\"" + inst_id + "\")] is missing");
      }
      auto old_clazz = JsonGetClassName(ROCKSDB_FUNC, oi_iter->second.params);
      auto new_clazz = JsonGetClassName(ROCKSDB_FUNC, value);
      if (new_clazz == old_clazz) {
      #if defined(NDEBUG)
        try {
      #endif
          PluginUpdate(existing, field, json(), value, repo);
          oi_iter->second.params.merge_patch(value);
          continue; // done for current item
      #if defined(NDEBUG)
        }
        catch (const Status& st) {
          // not found updater, overwrite with merged json
          oi_iter->second.params.merge_patch(value);
          value.swap(oi_iter->second.params);
        }
      #endif
      }
      field.p2name.erase(GetRawPtr(existing));
    }
    // do not use ObtainPlugin, to disallow define var2 = var1
    Ptr p = PluginFactory<Ptr>::AcquirePlugin(value, repo);
    if (!p) {
      THROW_InvalidArgument(
          "fail to AcquirePlugin: inst_id = " + inst_id +
              ", value_js = " + value.dump());
    }
    existing = p;
    if (value.is_string()) {
      value = json::object({{"class", value}, {"params", {}}});
    }
    field.p2name.emplace(GetRawPtr(p),
        SidePluginRepo::Impl::ObjInfo{inst_id, std::move(value)});
  }
}

template<class Ptr>
static void Impl_ImportOptions(SidePluginRepo::Impl::ObjMap<Ptr>& field,
                   const std::string& option_class_name,
                   const json& main_js, const SidePluginRepo& repo) {
  auto iter = main_js.find(option_class_name);
  if (main_js.end() == iter) {
    return;
  }
  if (!iter.value().is_object()) {
    THROW_InvalidArgument(option_class_name + " must be a json object");
  }
  for (auto& item : iter.value().items()) {
    const string& option_name = item.key();
    json params_js = item.value();
    auto ib = field.name2p->emplace(option_name, Ptr(nullptr));
    auto& existing = ib.first->second;
    if (!ib.second) { // existed
      assert(Ptr(nullptr) != existing);
      auto oi_iter = field.p2name.find(GetRawPtr(existing));
      if (field.p2name.end() == oi_iter) {
        THROW_Corruption("p2name[ptr_of(\"" + option_name + "\")] is missing");
      }
      PluginUpdate(existing, field, {}, params_js, repo);
      oi_iter->second.params["params"].merge_patch(params_js);
    }
    else {
      Ptr p = PluginFactory<Ptr>::AcquirePlugin(option_class_name, params_js, repo);
      assert(Ptr(nullptr) != p);
      existing = p;
      field.p2name.emplace(GetRawPtr(p),
          SidePluginRepo::Impl::ObjInfo{option_name, json{
              { "class", option_class_name},
              { "params", std::move(params_js)}
          }});
    }
  }
}

void SidePluginRepo::CleanResetRepo() {
  ROCKSDB_VERIFY_F(m_impl->db.name2p->empty(), "db not closed");
  m_impl.reset(new Impl);
}
SidePluginRepo::SidePluginRepo() noexcept {
  m_impl.reset(new Impl);
}
SidePluginRepo::~SidePluginRepo() {
  ROCKSDB_VERIFY_F(m_impl->db.name2p->empty(), "db not closed");
}

// yaml or json
Status SidePluginRepo::ImportAutoFile(const Slice& fname) {
  if (fname.ends_with(".json") || fname.ends_with(".js")) {
    return ImportJsonFile(fname);
  }
  if (fname.ends_with(".yaml") || fname.ends_with(".yml")) {
    return ImportYamlFile(fname);
  }
  return Status::InvalidArgument(ROCKSDB_FUNC,
              fname + " is unsupported file type");
}

std::string ReadWholeFile(const Slice& fname) {
  std::ifstream ifs(fname.data());
  if (!ifs.is_open()) {
    throw std::logic_error("open file failed: " + fname);
  }
  std::stringstream ss;
  ss << ifs.rdbuf();
  return ss.str();
}

Status SidePluginRepo::ImportJsonFile(const Slice& fname)
#if defined(NDEBUG)
try
#endif
{
  std::string json_str = ReadWholeFile(fname);
  return Import(json_str);
}
#if defined(NDEBUG)
catch (const std::exception& ex) {
  return Status::InvalidArgument(std::string(__FILE__)
                       + ":" ROCKSDB_PP_STR(__LINE__) ": "
                       + ROCKSDB_FUNC + ": file = " + fname, ex.what());
}
#endif

std::string YamlToJson(std::string& yaml_str);
Status SidePluginRepo::ImportYamlFile(const Slice& fname)
#if defined(NDEBUG)
try
#endif
{
  std::string json_str;
  {
    std::string yaml_str = ReadWholeFile(fname);
    json_str = YamlToJson(yaml_str);
  }
  return Import(json_str);
}
#if defined(NDEBUG)
catch (const std::exception& ex) {
  return Status::InvalidArgument(std::string(__FILE__)
                       + ":" ROCKSDB_PP_STR(__LINE__) ": "
                       + ROCKSDB_FUNC + ": file = " + fname, ex.what());
}
#endif

Status SidePluginRepo::Import(const string& json_str)
#if defined(NDEBUG)
try
#endif
{
  json js = json::parse(json_str);
  return Import(js);
}
#if defined(NDEBUG)
catch (const std::exception& ex) {
  // just parse error
  return Status::InvalidArgument(std::string(__FILE__)
                       + ":" ROCKSDB_PP_STR(__LINE__) ": "
                       + ROCKSDB_FUNC,
         Slice(ex.what()) + ": json_str is :\n" + json_str);
}
#endif

static
void MergeSubObject(json* target, const json& patch, const string& subname) {
  auto iter = patch.find(subname);
  if (patch.end() != iter) {
    auto& sub_js = iter.value();
    if (!sub_js.is_object()) {
      THROW_InvalidArgument("\"" + subname + "\" must be an object");
    }
    if (!target->is_null() && !target->is_object()) {
      THROW_Corruption(
          "\"target\" must be an object or null, subname = " + subname);
    }
    target->merge_patch(sub_js);
  }
}
static
void MergeSubAny(json* target, const json& patch, const string& subname) {
  auto iter = patch.find(subname);
  if (patch.end() != iter) {
    auto& sub_js = iter.value();
    target->merge_patch(sub_js);
  }
}

static void DoSetEnv(const std::string& name, const json& val, bool overwrite) {
  if (val.is_object()) {
    THROW_InvalidArgument("value of setenv must not be object");
  }
  else if (val.is_array()) {
    THROW_InvalidArgument("value of setenv must not be array");
  }
  else if (val.is_string()) {
    ::setenv(name.c_str(), val.get_ref<const std::string&>().c_str(), overwrite);
  }
  else if (val.is_boolean()) {
    ::setenv(name.c_str(), val.get<bool>() ? "1" : "0", overwrite);
  }
  else {
    const std::string& valstr = val.dump();
    ::setenv(name.c_str(), valstr.c_str(), overwrite);
  }
}

static void JS_setenv(const json& main_js) {
  auto iter = main_js.find("setenv");
  if (main_js.end() == iter) {
    return;
  }
  auto& envmap = iter.value();
  if (!envmap.is_object()) {
    THROW_InvalidArgument("main_js[\"setenv\"] must be a json object");
  }
  for (auto& item : envmap.items()) {
    const std::string& name = item.key();
    const json& val = item.value();
    if (SidePluginRepo::DebugLevel() >= 3) {
      const std::string& valstr = val.dump();
      fprintf(stderr, "JS_setenv: %s = %s\n", name.c_str(), valstr.c_str());
    }
    if (val.is_object()) {
      iter = val.find("value");
      if (val.end() == iter) {
        THROW_InvalidArgument("setenv[\"" + name + "\"][\"value\"] is missing");
      }
      bool overwrite = JsonSmartBool(val, "overwrite", false);
      DoSetEnv(name, iter.value(), overwrite);
    }
    else {
      DoSetEnv(name, val, false);
    }
  }
}

struct SideRepoImpl : SidePluginRepo::Impl {
  void ImportPermissions(const json& main_js) {
    auto iter = main_js.find("permissions");
    if (main_js.end() == iter) {
      return;
    }
    const json& js = iter.value();
    ROCKSDB_JSON_OPT_PROP(js, web_compact);
  }
};

Status SidePluginRepo::Import(const json& main_js)
#if defined(NDEBUG)
try
#endif
{
  JS_setenv(main_js);
  MergeSubObject(&m_impl->db_js, main_js, "databases");
  MergeSubObject(&m_impl->http_js, main_js, "http");
  MergeSubAny(&m_impl->open_js, main_js, "open");
  const auto& repo = *this;
#define JSON_IMPORT_REPO(Clazz, field) \
  Impl_Import(m_impl->field, #Clazz, main_js, repo)
  JSON_IMPORT_REPO(AnyPlugin                , any_plugin);
  JSON_IMPORT_REPO(Comparator               , comparator);
  JSON_IMPORT_REPO(Env                      , env);
  JSON_IMPORT_REPO(FileSystem               , file_system);
  JSON_IMPORT_REPO(Logger                   , info_log);
  JSON_IMPORT_REPO(SliceTransform           , slice_transform);
  JSON_IMPORT_REPO(Cache                    , cache);
  JSON_IMPORT_REPO(PersistentCache          , persistent_cache);
  JSON_IMPORT_REPO(CompactionExecutorFactory, compaction_executor_factory);
  JSON_IMPORT_REPO(CompactionFilterFactory  , compaction_filter_factory);
  JSON_IMPORT_REPO(ConcurrentTaskLimiter    , compaction_thread_limiter);
  JSON_IMPORT_REPO(EventListener            , event_listener);
  JSON_IMPORT_REPO(FileChecksumGenFactory   , file_checksum_gen_factory);
  JSON_IMPORT_REPO(FilterPolicy             , filter_policy);
  JSON_IMPORT_REPO(FlushBlockPolicyFactory  , flush_block_policy_factory);
  JSON_IMPORT_REPO(MergeOperator            , merge_operator);
  JSON_IMPORT_REPO(RateLimiter              , rate_limiter);
  JSON_IMPORT_REPO(SstFileManager           , sst_file_manager);
  JSON_IMPORT_REPO(SstPartitionerFactory    , sst_partitioner_factory);
  JSON_IMPORT_REPO(Statistics               , statistics);
  JSON_IMPORT_REPO(TablePropertiesCollectorFactory,
                   table_properties_collector_factory);

  JSON_IMPORT_REPO(MemoryAllocator          , memory_allocator);
  JSON_IMPORT_REPO(MemTableRepFactory       , memtable_factory);
  JSON_IMPORT_REPO(TableFactory             , table_factory);
  JSON_IMPORT_REPO(TransactionDBMutexFactory, txn_db_mutex_factory);
  JSON_IMPORT_REPO(WriteBufferManager       , write_buffer_manager);

  extern void DispatcherTableBackPatch(TableFactory*, const SidePluginRepo&);
  for (auto& kv : *m_impl->table_factory.name2p) {
    if (Slice(kv.second->Name()) == "DispatcherTable") {
      auto tf = kv.second.get();
      DispatcherTableBackPatch(tf, repo);
    }
  }

  extern void DynaMemTableBackPatch(MemTableRepFactory*, const SidePluginRepo&);
  for (auto& kv : *m_impl->memtable_factory.name2p) {
    if (Slice(kv.second->Name()) == "Dyna") {
      auto tf = kv.second.get();
      DynaMemTableBackPatch(tf, repo);
    }
  }

  Impl_ImportOptions(m_impl->db_options, "DBOptions", main_js, repo);
  Impl_ImportOptions(m_impl->cf_options, "CFOptions", main_js, repo);

  static_cast<SideRepoImpl*>(m_impl.get())->ImportPermissions(main_js);

  return Status::OK();
}
#if defined(NDEBUG)
catch (const std::exception& ex) {
  return Status::InvalidArgument(ROCKSDB_FUNC, ex.what());
}
catch (const Status& s) {
  return s;
}
#endif

template<class Ptr>
static void Impl_Export(const SidePluginRepo::Impl::ObjMap<Ptr>& field,
                   const char* name, json& main_js) {
  auto& field_js = main_js[name];
  for (auto& kv: field.p2name) {
    auto& params_js = field_js[kv.second.name];
    params_js = kv.second.params;
  }
}
Status SidePluginRepo::Export(json* main_js) const
#if defined(NDEBUG)
try
#endif
{
  assert(NULL != main_js);
#define JSON_EXPORT_REPO(Clazz, field) \
  Impl_Export(m_impl->field, #Clazz, *main_js)
  JSON_EXPORT_REPO(AnyPlugin                , any_plugin);
  JSON_EXPORT_REPO(Comparator               , comparator);
  JSON_EXPORT_REPO(Env                      , env);
  JSON_EXPORT_REPO(Logger                   , info_log);
  JSON_EXPORT_REPO(SliceTransform           , slice_transform);
  JSON_EXPORT_REPO(Cache                    , cache);
  JSON_EXPORT_REPO(PersistentCache          , persistent_cache);
  JSON_EXPORT_REPO(CompactionExecutorFactory, compaction_executor_factory);
  JSON_EXPORT_REPO(CompactionFilterFactory  , compaction_filter_factory);
  JSON_EXPORT_REPO(ConcurrentTaskLimiter    , compaction_thread_limiter);
  JSON_EXPORT_REPO(EventListener            , event_listener);
  JSON_EXPORT_REPO(FileChecksumGenFactory   , file_checksum_gen_factory);
  JSON_EXPORT_REPO(FileSystem               , file_system);
  JSON_EXPORT_REPO(FilterPolicy             , filter_policy);
  JSON_EXPORT_REPO(FlushBlockPolicyFactory  , flush_block_policy_factory);
  JSON_EXPORT_REPO(MergeOperator            , merge_operator);
  JSON_EXPORT_REPO(RateLimiter              , rate_limiter);
  JSON_EXPORT_REPO(SstFileManager           , sst_file_manager);
  JSON_EXPORT_REPO(SstPartitionerFactory    , sst_partitioner_factory);
  JSON_EXPORT_REPO(Statistics               , statistics);
  JSON_EXPORT_REPO(TablePropertiesCollectorFactory,
                   table_properties_collector_factory);

  JSON_EXPORT_REPO(MemoryAllocator          , memory_allocator);
  JSON_EXPORT_REPO(MemTableRepFactory       , memtable_factory);
  JSON_EXPORT_REPO(TableFactory             , table_factory);
  JSON_EXPORT_REPO(TransactionDBMutexFactory, txn_db_mutex_factory);
  JSON_EXPORT_REPO(WriteBufferManager       , write_buffer_manager);

  return Status::OK();
}
#if defined(NDEBUG)
catch (const std::exception& ex) {
  return Status::InvalidArgument(ROCKSDB_FUNC, ex.what());
}
#endif

Status SidePluginRepo::Export(string* json_str, bool pretty) const {
  assert(NULL != json_str);
  json js;
  Status s = Export(&js);
  if (s.ok()) {
    *json_str = js.dump(pretty ? 4 : -1);
  }
  return s;
}

template<class Map, class Ptr>
static void
Impl_Put(const std::string& name, Map& map, const Ptr& p) {
  auto& name2p = *map.name2p;
  if (p) { // put
    auto ib = name2p.emplace(name, p);
    if (!ib.second) {
      map.p2name.erase(GetRawPtr(ib.first->second));
      ib.first->second = p; // overwrite
    }
    map.p2name[GetRawPtr(ib.first->second)] =
        { name, json::object({{"manual", true}}) };
  }
  else { // p is null, do delete
    auto iter = name2p.find(name);
    if (name2p.end() == iter) {
      return;
    }
    map.p2name.erase(GetRawPtr(iter->second));
    name2p.erase(iter);
  }
}

template<class Map, class Ptr>
static bool
Impl_Get(const std::string& name, const Map& map, Ptr* pp) {
  auto& name2p = *map.name2p;
  auto iter = name2p.find(name);
  if (name2p.end() != iter) {
    *pp = iter->second;
    return true;
  }
  else {
    *pp = Ptr(nullptr);
    return false;
  }
}

template<class Map, class Ptr>
static const json*
Impl_GetConsParams(const Map& map, const Ptr& p) {
  auto iter = map.p2name.find(GetRawPtr(p));
  if (map.p2name.end() == iter) {
    //THROW_NotFound("p is not in repo");
    return nullptr;
  }
  return &iter->second.params;
}

#define JSON_REPO_TYPE_IMPL(field) \
void SidePluginRepo::Put(const string& name, \
                decltype((RepoPtrCref(((Impl*)0)->field))) p) { \
  Impl_Put(name, m_impl->field, p); \
} \
bool SidePluginRepo::Get(const string& name, \
                decltype(RepoPtrType(((Impl*)0)->field))* pp) const { \
  return Impl_Get(name, m_impl->field, pp); \
} \
const json* SidePluginRepo::GetConsParams( \
                decltype((RepoPtrCref(((Impl*)0)->field))) p) const { \
  return Impl_GetConsParams(m_impl->field, p); \
}

JSON_REPO_TYPE_IMPL(any_plugin)
JSON_REPO_TYPE_IMPL(cache)
JSON_REPO_TYPE_IMPL(persistent_cache)
JSON_REPO_TYPE_IMPL(compaction_executor_factory)
JSON_REPO_TYPE_IMPL(compaction_filter_factory)
JSON_REPO_TYPE_IMPL(comparator)
JSON_REPO_TYPE_IMPL(compaction_thread_limiter)
JSON_REPO_TYPE_IMPL(env)
JSON_REPO_TYPE_IMPL(event_listener)
JSON_REPO_TYPE_IMPL(file_checksum_gen_factory)
JSON_REPO_TYPE_IMPL(file_system)
JSON_REPO_TYPE_IMPL(filter_policy)
JSON_REPO_TYPE_IMPL(flush_block_policy_factory)
JSON_REPO_TYPE_IMPL(info_log)
JSON_REPO_TYPE_IMPL(memory_allocator)
JSON_REPO_TYPE_IMPL(memtable_factory)
JSON_REPO_TYPE_IMPL(merge_operator)
JSON_REPO_TYPE_IMPL(rate_limiter)
JSON_REPO_TYPE_IMPL(sst_file_manager)
JSON_REPO_TYPE_IMPL(sst_partitioner_factory)
JSON_REPO_TYPE_IMPL(statistics)
JSON_REPO_TYPE_IMPL(table_factory)
JSON_REPO_TYPE_IMPL(table_properties_collector_factory)
JSON_REPO_TYPE_IMPL(txn_db_mutex_factory)
JSON_REPO_TYPE_IMPL(write_buffer_manager)
JSON_REPO_TYPE_IMPL(slice_transform)

JSON_REPO_TYPE_IMPL(options)
JSON_REPO_TYPE_IMPL(db_options)
JSON_REPO_TYPE_IMPL(cf_options)

#define JSON_GetConsParams(field) \
const json* SidePluginRepo::GetConsParams( \
                decltype((RepoConstRawPtr(((Impl*)0)->field))) p) const { \
  return Impl_GetConsParams(m_impl->field, p); \
}

JSON_GetConsParams(any_plugin)
JSON_GetConsParams(cache)
JSON_GetConsParams(persistent_cache)
JSON_GetConsParams(compaction_executor_factory)
JSON_GetConsParams(compaction_filter_factory)
//JSON_GetConsParams(comparator)
JSON_GetConsParams(compaction_thread_limiter)
//JSON_GetConsParams(env)
JSON_GetConsParams(event_listener)
JSON_GetConsParams(file_checksum_gen_factory)
JSON_GetConsParams(file_system)
JSON_GetConsParams(filter_policy)
JSON_GetConsParams(flush_block_policy_factory)
JSON_GetConsParams(info_log)
JSON_GetConsParams(memory_allocator)
JSON_GetConsParams(memtable_factory)
JSON_GetConsParams(merge_operator)
JSON_GetConsParams(rate_limiter)
JSON_GetConsParams(sst_file_manager)
JSON_GetConsParams(sst_partitioner_factory)
JSON_GetConsParams(statistics)
JSON_GetConsParams(table_factory)
JSON_GetConsParams(table_properties_collector_factory)
JSON_GetConsParams(txn_db_mutex_factory)
JSON_GetConsParams(write_buffer_manager)
JSON_GetConsParams(slice_transform)

JSON_GetConsParams(options)
JSON_GetConsParams(db_options)
JSON_GetConsParams(cf_options)

void SidePluginRepo::Put(const std::string& name, DB* db) {
  Impl_Put(name, m_impl->db, DB_Ptr(db));
}
void SidePluginRepo::Put(const std::string& name, DB_MultiCF* db) {
  Impl_Put(name, m_impl->db, DB_Ptr(db));
}
bool SidePluginRepo::Get(const std::string& name, DB** db, Status* s) const {
  DB_Ptr dbp(nullptr);
  if (Impl_Get(name, m_impl->db, &dbp)) {
    if (!dbp.dbm) {
      *db = dbp.db;
      return true;
    }
    Status ss = Status::InvalidArgument(ROCKSDB_FUNC,
        "database \"" + name + "\" must be DB, but is DB_MultiCF");
    if (s)
      *s = ss;
    else
      throw ss; // NOLINT
  }
  return false;
}
bool SidePluginRepo::Get(const std::string& name, DB_MultiCF** db, Status* s) const {
  DB_Ptr dbp(nullptr);
  if (Impl_Get(name, m_impl->db, &dbp)) {
    if (dbp.dbm) {
      *db = dbp.dbm;
      return true;
    }
    Status ss = Status::InvalidArgument(ROCKSDB_FUNC,
        "database \"" + name + "\" must be DB_MultiCF, but is DB");
    if (s)
      *s = ss;
    else
      throw ss; // NOLINT
  }
  return false;
}

bool SidePluginRepo::Get(const std::string& name, DB_Ptr* dbp) const {
  std::lock_guard<std::mutex> lock(m_impl->db_mtx);
  return Impl_Get(name, m_impl->db, dbp);
}

template<class DBT>
static
Status OpenDB_tpl(SidePluginRepo& repo, const json& js, DBT** dbp);

/**
 * @param js may be:
 *  1. string name ref to a db defined in 'this' repo
 *  2. { "DB::Open": { options: {...} } }
 *  3. { "SomeStackableDB::Open": { } } }
 *  4. string name ref target in repo looks like:
 *     db : {
 *       dbname1 : {
 *         method : "DB::Open",
 *         params : {
 *           name : "some-name",
 *           path : "some-path",
 *           options: { ... }
 *         }
 *       },
 *       dbname2 : {
 *         method : "SomeStackableDB::Open",
 *         params : {
 *           name : "some-name",
 *           path : "some-path",
 *           options: { ... }
 *         }
 *       }
 *       dbname3 : {
 *         method : "DB::OpenReadOnly",
 *         params : {
 *           name : "some-name",
 *           path : "some-path",
 *           options: { ... }
 *         }
 *       }
 *     }
 */
Status SidePluginRepo::OpenDB(const json& js, DB** dbp) {
  return OpenDB_tpl<DB>(*this, js, dbp);
}
Status SidePluginRepo::OpenDB(const json& js, DB_MultiCF** dbp) {
  return OpenDB_tpl<DB_MultiCF>(*this, js, dbp);
}

Status SidePluginRepo::OpenDB(const std::string& js, DB** dbp)
#if defined(NDEBUG)
try
#endif
{
  return OpenDB_tpl<DB>(*this, js, dbp);
}
#if defined(NDEBUG)
catch (const std::exception& ex) {
  return Status::InvalidArgument(ROCKSDB_FUNC, "bad json object");
}
#endif
Status SidePluginRepo::OpenDB(const std::string& js, DB_MultiCF** dbp)
#if defined(NDEBUG)
try
#endif
{
  return OpenDB_tpl<DB_MultiCF>(*this, js, dbp);
}
#if defined(NDEBUG)
catch (const std::exception& ex) {
  return Status::InvalidArgument(ROCKSDB_FUNC, "bad json object");
}
#endif

inline DB* GetDB(DB* db) { return db; }
inline DB* GetDB(DB_MultiCF* db) { return db->db; }

template<class DBT>
static void Impl_OpenDB_tpl(const std::string& dbname,
                            const json& db_open_js,
                            SidePluginRepo& repo,
                            DBT** dbp) {
  auto iter = db_open_js.find("method");
  if (db_open_js.end() == iter) {
    THROW_InvalidArgument(
        "dbname = \"" + dbname + "\", param \"method\" is missing");
  }
  const std::string& method = iter.value().get<string>();
  iter = db_open_js.find("params");
  if (db_open_js.end() == iter) {
    THROW_InvalidArgument(
        "dbname = \"" + dbname + "\", param \"params\" is missing");
  }
  auto params_js = iter.value();
  if (!params_js.is_object()) {
    THROW_InvalidArgument(
        "dbname = \"" + dbname + "\", \"params\" must be a json object");
  }
  if (!dbname.empty()) {
    params_js["name"] = dbname;
  }
  { // dbname of rocksdb is really the db's default dir path.
    // And MANIFEST is always in the dir specified by dbname,
    // so dbname in rocksdb can be /some/path/to/db_dir, this makes
    // some confusion, so we allowing the path to be explicitly defined
    // in params, and keep using dbname as default 'path'.
    auto ib = params_js.emplace("path", dbname);
    if (!ib.second && !ib.first->is_string()) {
      THROW_InvalidArgument(
        "dbname = '" + dbname + "', params[path] must be a string if defined");
    }
  }
  auto& dbmap = repo.m_impl->db;

  // CompactionFilter ... may find db on opening
  std::lock_guard<std::mutex> lock(repo.m_impl->db_mtx);

  auto ib = dbmap.name2p->emplace(dbname, DB_Ptr(nullptr));
  if (!ib.second) {
    THROW_InvalidArgument("dup dbname = " + dbname);
  }
  if (SidePluginRepo::DebugLevel() >= 2) {
    fprintf(stderr, "%s:%d: Impl_OpenDB_tpl(): dbname = %s, params = %s\n",
            __FILE__, __LINE__, dbname.c_str(), params_js.dump(4).c_str());
  }
  // will open db by calling acq func such as DB::Open
  auto db = PluginFactory<DBT*>::AcquirePlugin(method, params_js, repo);
  assert(nullptr != db);
  ib.first->second = DB_Ptr(db);
  auto ib2 = dbmap.p2name.emplace(GetDB(db),
    decltype(dbmap.p2name.end()->second) {
      dbname,
      json::object({
        { "class", method }, // "method" is used as "class"
        { "params", std::move(params_js) }
      })
    });
  ROCKSDB_VERIFY(ib2.second);
  *dbp = db;
}

template<class DBT>
static
Status OpenDB_tpl(SidePluginRepo& repo, const json& js, DBT** dbp)
#if defined(NDEBUG)
try
#endif
{
  *dbp = nullptr;
  auto open_defined_db = [&](const std::string& dbname) {
      auto iter = repo.m_impl->db_js.find(dbname);
      if (repo.m_impl->db_js.end() == iter) {
        THROW_NotFound("dbname = \"" + dbname + "\" is not found");
      }
      Impl_OpenDB_tpl(dbname, iter.value(), repo, dbp);
  };
  if (js.is_string()) {
    const auto& str_val = js.get_ref<const std::string&>();
    if (str_val.empty()) {
      return Status::InvalidArgument(ROCKSDB_FUNC,
        "open js:string = \"" + str_val + "\" is empty");
    }
    if ('$' == str_val[0]) {
      if (str_val.size() < 3) {
        return Status::InvalidArgument(ROCKSDB_FUNC,
            "dbname = \"" + str_val + "\" is too short");
      }
      open_defined_db(PluginParseInstID(str_val));
    } else {
      // string which does not like ${dbname} or $dbname
      open_defined_db(str_val); // str_val is dbname
    }
  } else if (js.is_object()) {
    // when name is empty, js["params"]["name"] must be defined
    // this happens on OpenDB with a json
    auto i1 = js.find("params");
    if (js.end() == i1) {
      THROW_InvalidArgument(R"(missing "params": )" + js.dump());
    }
    auto i2 = i1.value().find("name");
    if (js.end() == i2) {
      THROW_InvalidArgument("missing params.name: " + js.dump());
    }
    if (!i2.value().is_string()) {
      THROW_InvalidArgument("params.name must be string: " + js.dump());
    }
    const auto& name = i2.value().get_ref<const std::string&>();
    if (name.empty()) {
      THROW_InvalidArgument("params.name must not be empty: " + js.dump());
    }
    std::string empty_name = ""; // NOLINT
    Impl_OpenDB_tpl(empty_name, js, repo, dbp);
  }
  else {
    THROW_InvalidArgument("bad js = " + js.dump());
  }
  return Status::OK();
}
#if defined(NDEBUG)
catch (const std::exception& ex) {
  return Status::InvalidArgument(ROCKSDB_FUNC, ex.what());
}
catch (const Status& s) {
  return s;
}
#endif

Status SidePluginRepo::OpenAllDB()
#if defined(NDEBUG)
try
#endif
{
  size_t num = 0;
  for (auto& item : m_impl->db_js.items()) {
    const std::string& dbname = item.key();
    const json& db_open_js = item.value();
    auto iter = db_open_js.find("params");
    if (db_open_js.end() == iter) {
      return Status::InvalidArgument(ROCKSDB_FUNC,
          "dbname = \"" + dbname + R"(", param "params" is missing)");
    }
    const json& params_js = iter.value();
    if (!params_js.is_object()) {
      return Status::InvalidArgument(ROCKSDB_FUNC,
          "dbname = \"" + dbname + R"(", "params" must be a json object)");
    }
    iter = params_js.find("column_families");
    if (params_js.end() == iter) {
      DB* db = Get(dbname);
      if (db) {
        return Status::InvalidArgument(ROCKSDB_FUNC,
            "DB \"" + dbname + "\" have been opened, can not open same db twice");
      }
      Impl_OpenDB_tpl(dbname, db_open_js, *this, &db);
    }
    else {
      DB_MultiCF* db = Get(dbname);
      if (db) {
        return Status::InvalidArgument(ROCKSDB_FUNC,
            "DB_MultiCF \"" + dbname + "\" have been opened, can not open same db twice");
      }
      Impl_OpenDB_tpl(dbname, db_open_js, *this, &db);
    }
    num++;
  }
  if (0 == num) {
    return Status::InvalidArgument(ROCKSDB_FUNC, "databases are empty");
  }
  return Status::OK();
}
#if defined(NDEBUG)
catch (const std::exception& ex) {
  return Status::InvalidArgument(ROCKSDB_FUNC, ex.what());
}
catch (const Status& s) {
  // nested Status
  return Status::InvalidArgument(ROCKSDB_FUNC, s.ToString());
}
#endif

std::shared_ptr<std::map<std::string, DB_Ptr> >
SidePluginRepo::GetAllDB() const {
  return m_impl->db.name2p;
}

/**
 * @param json_str sub object "open" is used as json_obj in
 *                 SidePluginRepo::OpenDB
 */
Status SidePluginRepo::OpenDB(DB** db) {
  const auto& open_js = m_impl->open_js;
  if (open_js.is_string() || open_js.is_object())
    return OpenDB(m_impl->open_js, db);
  else
    return Status::InvalidArgument(
        ROCKSDB_FUNC, "bad json[\"open\"] = " + open_js.dump());
}
Status SidePluginRepo::OpenDB(DB_MultiCF** db) {
  const auto& open_js = m_impl->open_js;
  if (open_js.is_string() || open_js.is_object())
    return OpenDB(open_js, db);
  else
    return Status::InvalidArgument(
        ROCKSDB_FUNC, "bad json[\"open\"] = " + open_js.dump());
}

Status SidePluginRepo::StartHttpServer()
#if defined(NDEBUG)
try
#endif
{
  const auto& http_js = m_impl->http_js;
  if (SidePluginRepo::DebugLevel() >= 2) {
    fprintf(stderr, "INFO: http_js = %s\n", http_js.dump().c_str());
  }
  if (http_js.is_object()) {
    m_impl->http.Init(http_js, this);
  }
  else if (!http_js.is_null()) {
    if (DebugLevel() >= 2) {
      fprintf(stderr, "ERROR: bad http_js = %s\n", http_js.dump().c_str());
    }
    return Status::InvalidArgument(
        ROCKSDB_FUNC, "bad http_js = " + http_js.dump());
  }
  return Status::OK();
}
#if defined(NDEBUG)
catch (const std::exception& ex) {
  return Status::InvalidArgument(ROCKSDB_FUNC, ex.what());
}
catch (const Status& s) {
  // nested Status
  return Status::InvalidArgument(ROCKSDB_FUNC, s.ToString());
}
#endif

void SidePluginRepo::CloseHttpServer() {
  if (SidePluginRepo::DebugLevel() >= 2) {
    fprintf(stderr, "INFO: CloseHttpServer(): http_js = %s\n",
            m_impl->http_js.dump().c_str());
  }
  m_impl->http.Close();
}

SidePluginRepo::Impl::Impl() {
}
SidePluginRepo::Impl::~Impl() {
}

void AnyPluginManip::Update(AnyPlugin* p, const json& q, const json& js,
                            const SidePluginRepo& repo) const {
  p->Update(q, js, repo);
}
std::string AnyPluginManip::ToString(const AnyPlugin& ap,
                                     const json& dump_options,
                                     const SidePluginRepo& repo) const {
  return ap.ToString(dump_options, repo);
}

std::string PluginParseInstID(const std::string& str_val) {
  // ${inst_id} or $inst_id
  if ('{' == str_val[1])
    return str_val.substr(2, str_val.size() - 3);
  else
    return str_val.substr(1, str_val.size() - 1);
}

ParseSizeXiB::ParseSizeXiB(const char* s) {
  if ('-' == s[0])
    m_val = ParseInt64(s);
  else
    m_val = ParseUint64(s);
}
ParseSizeXiB::ParseSizeXiB(const std::string& s) {
  if ('-' == s[0])
    m_val = ParseInt64(s);
  else
    m_val = ParseUint64(s);
}
ParseSizeXiB::ParseSizeXiB(const json& js) {
  if (js.is_number_integer())
    m_val = js.get<long long>();
  else if (js.is_number_unsigned())
    m_val = js.get<unsigned long long>();
  else if (js.is_string())
    *this = ParseSizeXiB(js.get_ref<const std::string&>());
  else
    throw std::invalid_argument("bad json = " + js.dump());
}
ParseSizeXiB::ParseSizeXiB(const json& js, const char* key) {
    if (!js.is_object()) {
      throw std::invalid_argument(
          std::string(ROCKSDB_FUNC) + ": js is not an object, key = " + key);
    }
    auto iter = js.find(key);
    if (js.end() != iter) {
      auto& sub_js = iter.value();
      if (sub_js.is_number_integer())
        m_val = sub_js.get<long long>();
      else if (sub_js.is_number_unsigned())
        m_val = sub_js.get<unsigned long long>();
      else if (sub_js.is_string())
        *this = ParseSizeXiB(sub_js.get_ref<const std::string&>());
      else
        throw std::invalid_argument(
                "bad sub_js = " + sub_js.dump() + ", key = \"" + key + "\"");
    }
    else {
      throw std::invalid_argument(
          std::string("ParseSizeXiB : not found key: \"") +
            key + "\" in js = " + js.dump());
    }
}

ParseSizeXiB::operator int() const {
  if (m_val < INT_MIN || m_val > INT_MAX)
    throw std::domain_error(std::string(ROCKSDB_FUNC) + ": out of range<int>");
  return (int)m_val;
}

ParseSizeXiB::operator long() const {
  if (sizeof(long) != sizeof(long long) && (m_val < LONG_MIN || m_val > LONG_MAX))
    throw std::domain_error(std::string(ROCKSDB_FUNC) + ": out of range<long>");
  return (long)m_val;
}
ParseSizeXiB::operator long long() const {
  return m_val;
}
ParseSizeXiB::operator unsigned int() const {
  if (m_val > UINT_MAX)
    throw std::domain_error(std::string(ROCKSDB_FUNC) + ": out of range<uint>");
  return (unsigned int)m_val;
}
ParseSizeXiB::operator unsigned long() const {
  if (sizeof(long) != sizeof(long long) && (unsigned long long)m_val > ULONG_MAX)
    throw std::domain_error(std::string(ROCKSDB_FUNC) + ": out of range<ulong>");
  return (unsigned long)m_val;
}
ParseSizeXiB::operator unsigned long long() const {
  return (unsigned long long)m_val;
}

void TableFactoryDummyFuncToPreventGccDeleteSymbols();
static int InitOnceDebugLevel() {
  const char* env = getenv("JsonOptionsRepo_DebugLevel");
  if (env) {
    return atoi(env);
  }
  TableFactoryDummyFuncToPreventGccDeleteSymbols();
  return 0;
}

int SidePluginRepo::DebugLevel() {
  static int d = InitOnceDebugLevel();
  return d;
}

std::string SizeToString(unsigned long long val) {
  char buf[64];
  int shift = 0;
  char unit = 'X';
  if ((val >> 60) != 0) {
    shift = 60, unit = 'E';
  }
  else if ((val >> 50) != 0) {
    shift = 50, unit = 'P';
  }
  else if ((val >> 40) != 0) {
    shift = 40, unit = 'T';
  }
  else if ((val >> 30) != 0) {
    shift = 30, unit = 'G';
  }
  else if ((val >> 20) != 0) {
    shift = 20, unit = 'M';
  }
  else if ((val >> 10) != 0) {
    shift = 10, unit = 'K';
  }
  else {
    return std::string(buf, snprintf(buf, sizeof(buf), "%lld B", val));
  }
  auto fval = double(val) / (1LL << shift);
  return std::string(buf, snprintf(buf, sizeof(buf), "%.3f %ciB", fval, unit));
}

bool JsonSmartBool(const json& js) {
  if (js.is_string()) {
    const std::string& s = js.get_ref<const std::string&>();
    if (s.empty()) return true; // empty means true
    if (strcasecmp(s.c_str(), "true") == 0) return true;
    if (strcasecmp(s.c_str(), "false") == 0) return false;
    if (strcasecmp(s.c_str(), "on") == 0) return true;
    if (strcasecmp(s.c_str(), "off") == 0) return false;
    if (strcasecmp(s.c_str(), "yes") == 0) return true;
    if (strcasecmp(s.c_str(), "no") == 0) return false;
    char* endptr = nullptr;
    long val = strtol(s.c_str(), &endptr, 10);
    if (endptr && '\0' == *endptr) {
      return 0 != val;
    }
    throw std::invalid_argument("JsonSmartBool: bad js = " + s);
  }
  if (js.is_boolean()) return js.get<bool>();
  if (js.is_number_integer()) return js.get<long long>() != 0;
  if (js.is_array()) {
    // http param: a=1&a=2&a=3 will construct a json array
    // we take the last item of the json array
    if (js.size() == 0) {
      throw std::invalid_argument("JsonSmartBool: js is an empty json array");
    }
    return JsonSmartBool(js.back());
  }
  throw std::invalid_argument("JsonSmartBool: bad js = " + js.dump());
}

bool JsonSmartBool(const json& js, const char* subname, bool Default) {
  auto iter = js.find(subname);
  if (js.end() != iter) {
    return JsonSmartBool(iter.value());
  }
  return Default;
}
void JsonSmartBool(bool* result, const json& js, const char* subname) {
  auto iter = js.find(subname);
  if (js.end() != iter) {
    *result = JsonSmartBool(iter.value());
  }
}

int JsonSmartInt(const json& js) {
  if (js.is_string()) {
    const std::string& s = js.get_ref<const std::string&>();
    if (isdigit((unsigned char)s[0])) {
      return atoi(s.c_str());
    }
    throw std::invalid_argument("JsonSmartInt: bad js = " + s);
  }
  if (js.is_number_integer()) return js.get<int>();
  if (js.is_array()) {
    // http param: a=1&a=2&a=3 will construct a json array
    // we take the last item of the json array
    if (js.size() == 0) {
      throw std::invalid_argument("JsonSmartInt: js is an empty json array");
    }
    return JsonSmartInt(js.back());
  }
  throw std::invalid_argument("JsonSmartInt: bad js = " + js.dump());
}

int JsonSmartInt(const json& js, const char* subname, int Default) {
  auto iter = js.find(subname);
  if (js.end() != iter) {
    return JsonSmartInt(iter.value());
  }
  return Default;
}
void JsonSmartInt(int* result, const json& js, const char* subname) {
  auto iter = js.find(subname);
  if (js.end() != iter) {
    *result = JsonSmartInt(iter.value());
  }
}

int64_t JsonSmartInt64(const json& js) {
  if (js.is_string()) {
    const std::string& s = js.get_ref<const std::string&>();
    if (isdigit((unsigned char)s[0])) {
      return atoi(s.c_str());
    }
    throw std::invalid_argument("JsonSmartInt64: bad js = " + s);
  }
  if (js.is_number_integer()) return js.get<int>();
  if (js.is_array()) {
    // http param: a=1&a=2&a=3 will construct a json array
    // we take the last item of the json array
    if (js.size() == 0) {
      throw std::invalid_argument("JsonSmartInt: js is an empty json array");
    }
    return JsonSmartInt64(js.back());
  }
  throw std::invalid_argument("JsonSmartInt64: bad js = " + js.dump());
}

int64_t JsonSmartInt64(const json& js, const char* subname, int64_t Default) {
  auto iter = js.find(subname);
  if (js.end() != iter) {
    return JsonSmartInt64(iter.value());
  }
  return Default;
}
void JsonSmartInt64(int64_t* result, const json& js, const char* subname) {
  auto iter = js.find(subname);
  if (js.end() != iter) {
    *result = JsonSmartInt64(iter.value());
  }
}

static void JsonToHtml_Object(const json& arr, std::string& html, bool nested);
static void JsonToHtml_Array(const json& arr, std::string& html) {
  size_t cnt = 0;
  for (const auto& kv : arr.items()) {
    if (cnt++)
      html.append("<tr><td>");
    else
      html.append("<td>"); // first elem

    const auto& val = kv.value();
    if (val.is_object())
      JsonToHtml_Object(val, html, true);
    else if (val.is_string())
      html.append(val.get_ref<const std::string&>());
    else // nested array also use dump
      html.append(val.dump());

    html.append("</td></tr>\n");
  }
}

static void JsonToHtml_ArrayCol(const json& arr, std::string& html) {
  // columns has order
  std::vector<std::string> colnames;
  for (auto& kv : arr[0]["<htmltab:col>"].items()) {
    colnames.push_back(kv.value());
  }
//  html.append("<table border=1 width=\"100%\"><tbody>\n");
  html.append("<table border=1><tbody>\n");
  html.append("<tr>");
  for (const auto& colname: colnames) {
    html.append("<th>");
    html.append(colname);
    html.append("</th>");
  }
  html.append("</tr>\n");
  size_t row = 0;
  for (auto& item : arr.items()) {
    html.append("<tr>");
    const auto& row_js = item.value();
    for (auto& colname : colnames) {
      auto iter = row_js.find(colname);
      if (row_js.end() == iter) {
        throw std::invalid_argument(
            "JsonToHtml_ArrayCol: array element are not homogeneous: missing colname = "
            + colname + ", row = "  + row_js.dump()
            + ", colnames = " + arr[0]["<htmltab:col>"].dump());
      }
      const json& val = iter.value();
      html.append("<td>");
      if (val.is_string())
        html.append(val.get_ref<const std::string&>());
      else
        html.append(val.dump());
      html.append("</td>");
    }
    row++;
    html.append("</tr>\n");
  }
  html.append("</tbody></table>\n");
}

static void JsonToHtml_Object(const json& obj, std::string& html, bool nested) {
  if (nested && false) // dont 100% width
    html.append("<table border=1 width=\"100%\"><tbody>\n");
  else
    html.append("<table border=1><tbody>\n");
  //html.append("<tr><th>name</th><th>value</th></tr>\n");
  for (const auto& kv : obj.items()) {
    const std::string& key = kv.key();
    const auto& val = kv.value();
    if (val.is_object()) {
      html.append("<tr><th>");
      html.append(key);
      html.append("</th><td>\n");
      JsonToHtml_Object(val, html, true);
      html.append("</td></tr>\n");
    }
    else if (val.is_array()) {
      if (val.size() > 0 && val[0].contains("<htmltab:col>")) {
        html.append("<tr><th>");
        html.append(key);
        html.append("</th><td>\n");
        JsonToHtml_ArrayCol(val, html);
        html.append("</td></tr>\n");
      }
      else {
        char buf[64];
        html.append(buf, snprintf(buf, sizeof(buf), "<tr><th rowspan=%zd>", val.size()));
        html.append(key);
        html.append("</th>\n");
        JsonToHtml_Array(val, html);
      }
    }
    else {
      html.append("<tr><td>");
      html.append(key);
      html.append("</td><td>");
      if (val.is_string())
        html.append(val.get_ref<const std::string&>());
      else if (val.is_number()) {
        //html.replace(html.size()-4, 4, "<td align='right'>");
        if (val.is_number_float()) {
          char buf[64];
          auto len = snprintf(buf, sizeof buf, "%.6f", val.get<double>());
          html.append(buf, len);
        } else {
          html.append(val.dump());
        }
      }
      else
        html.append(val.dump());
      html.append("</td></tr>\n");
    }
  }
  html.append("</tbody></table>\n");
}

std::string JsonToHtml(const json& obj) {
  std::string html;
  if (obj.is_structured()) {
    JsonToHtml_Object(obj, html, false);
  }
  return html;
}

std::string JsonToString(const json& obj, const json& options) {
  if (obj.is_string()) {
    return obj.get_ref<const std::string&>();
  }
  int indent = -1;
  auto iter = options.find("pretty");
  if (options.end() != iter) {
    if (JsonSmartBool(iter.value())) {
      indent = 4;
    }
  }
  iter = options.find("indent");
  if (options.end() != iter) {
    indent = JsonSmartInt(iter.value());
  }
  if (-1 != indent) {
    fprintf(stderr, "INFO: JsonToString: indent = %d\n", indent);
  }
  if (JsonSmartBool(options, "html", true))
    return JsonToHtml(obj);
  else
    return obj.dump(indent);
}

std::string
PluginToString(const DB_Ptr& dbp,
               const SidePluginRepo::Impl::ObjMap<DB_Ptr>& map,
               const json& js, const SidePluginRepo& repo) {
  auto iter = map.p2name.find(dbp.db);
  if (map.p2name.end() != iter) {
    if (dbp.dbm) {
      auto manip = PluginManip<DB_MultiCF>::AcquirePlugin(iter->second.params, repo);
      return manip->ToString(*dbp.dbm, js, repo);
    }
    else {
      auto manip = PluginManip<DB>::AcquirePlugin(iter->second.params, repo);
      return manip->ToString(*dbp.db, js, repo);
    }
  }
  THROW_NotFound("db ptr is not in repo");
}

static void append_varname(std::string& res, const std::string& varname) {
  if ('$' == varname[0]) {
    if ('{' == varname[1])
      res.append(varname.data() + 2, varname.size() - 3);
    else
      res.append(varname.data() + 1, varname.size() - 1);
  } else {
    res.append(varname);
  }
}

std::string
JsonRepoGetHtml_ahref(const char* mapname, const std::string& varname) {
  // <a href='/mapname/varname'>${varname}</a>
  size_t maplen = strlen(mapname);
  std::string link;
  link.reserve(maplen + 2 * varname.size() + 64);
  link.append("<a href='/");
  link.append(mapname, maplen);
  link.push_back('/');
  append_varname(link, varname);
  link.append("?html=1'>${");
  append_varname(link, varname);
  link.append("}</a>");
  return link;
}

void
JsonRepoSetHtml_ahref(json& js, const char* mapname, const std::string& varname) {
  js = JsonRepoGetHtml_ahref(mapname, varname);
}

void JsonRepoSet(json& js, const void* prop,
                 const std::map<const void*, SidePluginRepo::Impl::ObjInfo>& p2name,
                 const char* mapname, bool html) {
  auto iter = p2name.find(prop);
  if (p2name.end() != iter) {
    ROCKSDB_VERIFY(nullptr != prop);
    if (iter->second.name.empty())
      js = iter->second.params;
    else if (html)
      JsonRepoSetHtml_ahref(js, mapname, iter->second.name);
    else
      js = "${" + iter->second.name + "}";
  }
  else if (nullptr == prop) {
    js = "null";
  }
  else {
    js = "$(BuiltinDefault)";
  }
}

std::string demangle(const char* name) {
#ifdef _MSC_VER
  return name;
#elif defined(__GNUC__)
  int status = -4; // some arbitrary value to eliminate the compiler warning
  char* res = abi::__cxa_demangle(name, NULL, NULL, &status);
  std::string dem = (status == 0) ? res : name;
  free(res);
  return dem;
#else
  return boost::core::demangle(name);
#endif
}

std::string demangle(const std::type_info& ti) {
  return demangle(ti.name());
}


} // ROCKSDB_NAMESPACE
