#include "side_plugin_factory.h"
#include <thread>
#include <shared_mutex>

namespace ROCKSDB_NAMESPACE {

struct DB_MultiCF_Impl : public DB_MultiCF {
  DB_MultiCF_Impl(const SidePluginRepo*, const std::string& name, DB*, const std::vector<ColumnFamilyHandle*>&, int catch_up_delay_ms = -1);
  DB_MultiCF_Impl();
  ~DB_MultiCF_Impl() override;
  ColumnFamilyHandle* Get(const std::string& cfname) const override;
  Status CreateColumnFamily(const std::string& cfname, const std::string& json_str, ColumnFamilyHandle**) override;
  Status CreateColumnFamilyWithImport(
                  const std::string& cfname, const ImportColumnFamilyOptions&,
                  const std::vector<const ExportImportFilesMetaData*>&,
                  const std::string& json_str, ColumnFamilyHandle**);
  Status DropColumnFamilyImpl(const std::string& cfname, ColumnFamilyHandle**);
  Status DropColumnFamily(const std::string& cfname, bool del_cfh) override;
  Status DropColumnFamily(ColumnFamilyHandle*, bool del_cfh) override;
  std::vector<ColumnFamilyHandle*> get_cf_handles_view() const override;
  void AddOneCF_ToMap(const std::string& cfname, ColumnFamilyHandle*, const json&);
  void InitAddCF_ToMap(const json& js_cf_desc);
  SidePluginRepo::Impl::ObjMap<ColumnFamilyHandle*> m_cfhs;
  const SidePluginRepo* m_repo;
  std::function<ColumnFamilyHandle*
    (DB*, const std::string& cfname, const ColumnFamilyOptions&, const json& extra_args)
   > m_create_cf;
  std::function<ColumnFamilyHandle*
    (DB*, const std::string& cfname, const ColumnFamilyOptions&,
          const ImportColumnFamilyOptions& import_options,
          const std::vector<const ExportImportFilesMetaData*>& metadatas,
          const json& extra_args)
   > m_create_cf_with_import;
  std::unique_ptr<std::thread> m_catch_up_thread;
  bool m_catch_up_running;
  int m_catch_up_delay_ms;
  mutable std::shared_mutex m_mtx;
};
template<class Ptr>
Ptr ObtainOPT(SidePluginRepo::Impl::ObjMap<Ptr>& field,
              const char* option_class, // "DBOptions" or "CFOptions"
              const json& option_js, const SidePluginRepo& repo);


inline const SidePluginRepo& null_repo_ref() {
   SidePluginRepo* p = nullptr;
   return *p;
}

template<class Object>
SerDeFactory<Object>* SerDeFac(const Object*) { return nullptr; }
template<class Object>
SerDeFactory<Object>* SerDeFac(const std::shared_ptr<Object>&) { return nullptr; }

// Suffix 'Req' means 'required'
template<class Object>
void SerDe_SerializeReq(FILE* fp, const std::string& clazz, const Object* obj,
                        const json& js = json{},
                        const SidePluginRepo& repo = null_repo_ref()) {
  assert(nullptr != obj);
  auto serde = SerDeFactory<Object>::AcquirePlugin(clazz, js, repo);
  serde->Serialize(fp, *obj);
}
template<class Object>
void SerDe_SerializeReq(FILE* fp, const std::string& clazz,
                        const std::shared_ptr<Object>& obj,
                        const json& js = json{},
                        const SidePluginRepo& repo = null_repo_ref()) {
  return SerDe_SerializeReq(fp, clazz, obj.get(), js, repo);
}

// Suffix 'Opt' means 'optional'
// if clazz has no SerDeFunc, obj can     be null
// if clazz has    SerDeFunc, obj can not be null
template<class Object>
void SerDe_SerializeOpt(FILE* fp, const std::string& clazz, const Object* obj,
                        const json& js = json{},
                        const SidePluginRepo& repo = null_repo_ref()) {
  auto serde = SerDeFac(obj)->NullablePlugin(clazz, js, repo);
  if (serde) {
    assert(nullptr != obj);
    serde->Serialize(fp, *obj);
  }
}
template<class Object>
void SerDe_SerializeOpt(FILE* fp, const std::string& clazz,
                        const std::shared_ptr<Object>& obj,
                        const json& js = json{},
                        const SidePluginRepo& repo = null_repo_ref()) {
  return SerDe_SerializeOpt(fp, clazz, obj.get(), js, repo);
}

template<class T>
T* SidePlugin_const_cast(const T* p) { return const_cast<T*>(p); }

template<class Object>
void SerDe_DeSerialize(FILE* fp, const std::string& clazz, Object* obj,
                       const json& js = json{},
                       const SidePluginRepo& repo = null_repo_ref()) {
  auto serde = SerDeFac(obj)->NullablePlugin(clazz, js, repo);
  if (serde) {
    assert(nullptr != obj);
    // Object may be const, such as 'const Comparator'
    serde->DeSerialize(fp, SidePlugin_const_cast(obj));
  }
}
template<class Object>
void SerDe_DeSerialize(FILE* fp, const std::string& clazz,
                       const std::shared_ptr<Object>& obj,
                       const json& js = json{},
                       const SidePluginRepo& repo = null_repo_ref()) {
  SerDe_DeSerialize(fp, clazz, obj.get(), js, repo);
}
template<class Ptr>
void SerDe_DeSerialize(FILE* fp, const Ptr& p,
                       const json& js = json{},
                       const SidePluginRepo& repo = null_repo_ref()) {
  if (p)
    SerDe_DeSerialize(fp, p->Name(), p, js, repo);
}

} // namespace ROCKSDB_NAMESPACE
