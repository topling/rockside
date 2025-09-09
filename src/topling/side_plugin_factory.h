//
// Created by leipeng on 2020-06-29.
//
// side_plugin_repo.h    is mostly for plugin users
// side_plugin_factory.h is mostly for plugin developers
//
#pragma once

#include <mutex>
#include <shared_mutex>

#include "side_plugin_repo.h"
#include "web/json_civetweb.h"
#include "json.h"
#include "rocksdb/enum_reflection.h"
#include "rocksdb/preproc.h"

#include <terark/util/nolocks_localtime.hpp>

namespace ROCKSDB_NAMESPACE {

#define THROW_STATUS(Type, msg) throw rocksdb::Status::Type(std::string(__FILE__) + (":" ROCKSDB_PP_STR(__LINE__) ": ") + ROCKSDB_FUNC, msg)
#define THROW_InvalidArgument(msg) THROW_STATUS(InvalidArgument, msg)
#define THROW_Corruption(msg) THROW_STATUS(Corruption, msg)
#define THROW_NotFound(msg) THROW_STATUS(NotFound, msg)
#define THROW_NotSupported(msg) THROW_STATUS(NotSupported, msg)

#if !defined(TOPLINGDB_ENABLE_TRY_CATCH)
  #if defined(NDEBUG)
    #define TOPLINGDB_ENABLE_TRY_CATCH 1
  #else
    #define TOPLINGDB_ENABLE_TRY_CATCH 0
  #endif
#endif

#if TOPLINGDB_ENABLE_TRY_CATCH
  #define TOPLINGDB_TRY            try
  #define TOPLINGDB_CATCH(Except)  catch (Except)
#else
  #define TOPLINGDB_TRY            if (1)
  #define TOPLINGDB_CATCH(Except)  else if (0) try {} catch (Except)
#endif

template<class P> struct RemovePtr_tpl; // NOLINT
template<class T> struct RemovePtr_tpl<T*> { typedef T type; };
template<class T> struct RemovePtr_tpl<std::shared_ptr<T> > { typedef T type; };
template<> struct RemovePtr_tpl<DB_Ptr> { typedef DB type; };
template<class P> using RemovePtr = typename RemovePtr_tpl<P>::type;

template<class T> T* GetRawPtr(T* p){ return p; }
template<class T> T* GetRawPtr(const std::shared_ptr<T>& p){ return p.get(); }
inline DB* GetRawPtr(const DB_Ptr& p){ return p.db; }

using terark::StrDateTimeNow;
std::string demangle(const char* name);
std::string demangle(const std::type_info&);

struct CFPropertiesWebView {
  DB* db;
  ColumnFamilyHandle* cfh;
};
struct SidePluginRepo::Impl {
  Impl(const Impl&) = delete;
  Impl& operator=(const Impl&) = delete;
  Impl();
  ~Impl();

  struct ObjInfo {
    std::string name;
    json spec; // { class : "class_name", params : "params..." }
  };
  template<class Ptr>
  class ObjMap {
  public:
    ObjMap(const ObjMap&) = delete;
    ObjMap& operator=(const ObjMap&) = delete;
    ObjMap();
    ~ObjMap();
    std::map<const void*, ObjInfo> p2name;
    std::shared_ptr<std::map<std::string, Ptr> > name2p;
  };
  template<class T>
  using ObjRepo = ObjMap<std::shared_ptr<T> >;

  ObjRepo<AnyPlugin> any_plugin;
  ObjRepo<Cache> cache;
  ObjRepo<PersistentCache> persistent_cache;
  ObjRepo<CompactionExecutorFactory> compaction_executor_factory;
  ObjRepo<CompactionFilterFactory> compaction_filter_factory;
  ObjMap<const Comparator*> comparator;
  ObjRepo<ConcurrentTaskLimiter> compaction_thread_limiter;
  ObjMap<Env*> env;
  ObjRepo<EventListener> event_listener;
  ObjRepo<FileChecksumGenFactory> file_checksum_gen_factory;
  ObjRepo<FileSystem> file_system;
  ObjRepo<const FilterPolicy> filter_policy;
  ObjRepo<FlushBlockPolicyFactory> flush_block_policy_factory;
  ObjRepo<Logger> info_log;
  ObjRepo<MemoryAllocator> memory_allocator;
  ObjRepo<MemTableRepFactory> memtable_factory;
  ObjRepo<MergeOperator> merge_operator;
  ObjRepo<RateLimiter> rate_limiter;
  ObjRepo<SstFileManager> sst_file_manager;
  ObjRepo<SstPartitionerFactory> sst_partitioner_factory;
  ObjRepo<Statistics> statistics;
  ObjRepo<TableFactory> table_factory;
  ObjRepo<TablePropertiesCollectorFactory> table_properties_collector_factory;
  ObjRepo<TransactionDBMutexFactory> txn_db_mutex_factory;
  ObjRepo<WriteBufferManager> write_buffer_manager;
  ObjRepo<const SliceTransform> slice_transform;
  ObjRepo<WBWIFactory> wbwi_factory;

  ObjRepo<Options> options;
  ObjRepo<DBOptions> db_options;
  ObjRepo<ColumnFamilyOptions> cf_options;

  ObjRepo<CFPropertiesWebView> props;
  ObjMap<DB_Ptr> db;
  std::map<DB*, ColumnFamilyHandle*> keep_default_cf;
  std::mutex db_mtx;
  std::shared_mutex props_mtx;

  json db_js; // not evaluated during import
  json open_js;
  json http_js;

  JsonCivetServer http;
  bool web_compact = false;
  bool web_write = false;
};

///@note on principle, the factory itself is stateless, but its product
/// can has states, sometimes we need factory of factory, in this case,
/// just let the factory being PluginFactory:
/// class SomeClass : public PluginFactory<SomeClass*> {...};
/// class SomeClass : public PluginFactory<shared_ptr<SomeClass> > {...};
template<class Ptr>
class PluginFactory {
public:
  PluginFactory(const PluginFactory&) = delete;
  PluginFactory& operator=(const PluginFactory&) = delete;
  PluginFactory();
  virtual ~PluginFactory();
  // in some contexts Acquire means 'CreateNew'
  // in some contexts Acquire means 'GetExisting'
  static Ptr AcquirePlugin(const std::string& clazz, const json&,
                           const SidePluginRepo&);

  // json is string class_name or
  // object{ class: "class_name", params: {...} }
  // throw if not found
  static Ptr AcquirePlugin(const json&, const SidePluginRepo&);

  // not throw if plugin does not exist
  static Ptr NullablePlugin(const std::string& clazz, const json&,
                            const SidePluginRepo&);
  static Ptr NullablePlugin(const json&, const SidePluginRepo&);

  static Ptr ObtainPlugin(const char* varname, const char* func_name,
                          const json&, const SidePluginRepo&);

  static Ptr GetPlugin(const char* varname, const char* func_name,
                       const json&, const SidePluginRepo&);

  static bool HasPlugin(const std::string& class_name);
  static bool SamePlugin(const std::string& clazz1, const std::string& clazz2);

  typedef Ptr (*AcqFunc)(const json&, const SidePluginRepo&);
  struct Meta;
  struct Reg {
    Reg(const Reg&) = delete;
    Reg& operator=(const Reg&) = delete;
    Reg(const char* class_name, AcqFunc, const char* file, int line) noexcept;
    ~Reg();
    typename std::map<Slice, Meta>::iterator ipos;
    struct Impl;
  };
};
template<class Object>
using PluginFactorySP = PluginFactory<std::shared_ptr<Object> >;

template<class Object>
struct PluginManipFunc {
  PluginManipFunc(const PluginManipFunc&) = delete;
  PluginManipFunc& operator=(const PluginManipFunc&) = delete;
  PluginManipFunc() = default;
  virtual ~PluginManipFunc() {}
  virtual void Update(Object*, const json& query, const json& body, const SidePluginRepo&) const = 0;
  virtual std::string ToString(const Object&, const json&, const SidePluginRepo&) const = 0;
  using InterfaceType = PluginManipFunc;
};
template<class ManipClass>
static const typename ManipClass::InterfaceType*
PluginManipSingleton(const json&, const SidePluginRepo&) {
  static const ManipClass manip;
  return &manip;
}
#define ROCKSDB_REG_PluginManip(ClassName, ManipClass) \
  PluginFactory<const typename ManipClass::InterfaceType*>::Reg \
      ROCKSDB_PP_CAT_3(g_reg_manip_,ManipClass,__LINE__) \
     (ClassName, &PluginManipSingleton<ManipClass>, __FILE__, __LINE__)

template<class Object>
using PluginManip = PluginFactory<const PluginManipFunc<Object>*>;
template<class Ptr>
void PluginUpdate(const Ptr& p, const SidePluginRepo::Impl::ObjMap<Ptr>&,
                  const json& query, const json& body, const SidePluginRepo&);
void PluginUpdate(const DB_Ptr&, const SidePluginRepo::Impl::ObjMap<DB_Ptr>&,
                  const json& query, const json& body, const SidePluginRepo&);

template<class Ptr>
std::string
PluginToString(const Ptr& p, const SidePluginRepo::Impl::ObjMap<Ptr>& map,
               const json& js, const SidePluginRepo& repo);
std::string
PluginToString(const DB_Ptr&, const SidePluginRepo::Impl::ObjMap<DB_Ptr>& map,
               const json& js, const SidePluginRepo& repo);
std::string
PluginToString(const std::shared_ptr<CFPropertiesWebView>&,
               const SidePluginRepo::Impl::ObjRepo<CFPropertiesWebView>& map,
               const json& js, const SidePluginRepo& repo);

// use SerDeFunc as plugin, register SerDeFunc as plugin
template<class Object>
struct SerDeFunc {
  SerDeFunc(const SerDeFunc&) = delete;
  SerDeFunc& operator=(const SerDeFunc&) = delete;
  SerDeFunc() = default;
  virtual ~SerDeFunc() {}
  virtual void Serialize(FILE*, const Object&) const = 0;
  virtual void DeSerialize(FILE*, Object*) const = 0;
  using InterfaceType = SerDeFunc;
};
template<class Object>
struct DcompactSerDeFunc : SerDeFunc<Object> {
  virtual void Serialize(FILE* fp, const Object& obj) const override {
    if (!IsCompactionWorker())
      SerializeRequest(fp, obj); // phase 1, DB Side
    else
      SerializeResponse(fp, obj); // phase 3, compact worker side
  }
  virtual void DeSerialize(FILE* fp, Object* obj) const override {
    if (IsCompactionWorker())
      DeSerializeRequest(fp, obj); // phase 2, compact worker side
    else
      DeSerializeResponse(fp, obj); // phase 4, DB Side
  }
  virtual void SerializeRequest(FILE*, const Object&) const {
    ROCKSDB_VERIFY(!IsCompactionWorker()); // phase 1, DB Side
  }
  virtual void DeSerializeRequest(FILE*, Object*) const {
    ROCKSDB_VERIFY(IsCompactionWorker()); // phase 2, compact worker side
  }
  virtual void SerializeResponse(FILE*, const Object&) const {
    ROCKSDB_VERIFY(IsCompactionWorker()); // phase 3, compact worker side
  }
  virtual void DeSerializeResponse(FILE*, Object*) const {
    ROCKSDB_VERIFY(!IsCompactionWorker()); // phase 4, DB side
  }
};

template<class First, class... List>
struct SFINAE_FirstType {
	typedef First type;
};
template<class ConcretClass, class Interface>
auto JS_NewSidePlugin(const json&, const SidePluginRepo&) -> typename
SFINAE_FirstType<std::shared_ptr<Interface>, decltype(ConcretClass())>
::type {
  return std::make_shared<ConcretClass>();
}
template<class ConcretClass, class Interface>
auto JS_NewSidePlugin(const json& js, const SidePluginRepo& repo) -> typename
SFINAE_FirstType<std::shared_ptr<Interface>, decltype(ConcretClass(js, repo))>
::type {
  return std::make_shared<ConcretClass>(js, repo);
}
#define ROCKSDB_REG_PluginSerDe_2(ClassName, SerDeClass) \
  PluginFactory<std::shared_ptr<typename SerDeClass::InterfaceType> >::Reg \
      ROCKSDB_PP_CAT_3(g_reg_serde_,SerDeClass,__LINE__) \
     (ClassName, &JS_NewSidePlugin<SerDeClass, typename SerDeClass::InterfaceType>, __FILE__, __LINE__)
#define ROCKSDB_REG_PluginSerDe_1(ClassType) \
  PluginFactory<std::shared_ptr<typename ClassType##_SerDe::InterfaceType> >::Reg \
      ROCKSDB_PP_CAT_3(g_reg_serde_,ClassType##_SerDe,__LINE__) \
     (#ClassType, &JS_NewSidePlugin<ClassType##_SerDe, typename ClassType##_SerDe::InterfaceType>, __FILE__, __LINE__)
#define ROCKSDB_REG_PluginSerDe(...) ROCKSDB_PP_CAT2 \
       (ROCKSDB_REG_PluginSerDe_,ROCKSDB_PP_ARG_N(__VA_ARGS__))(__VA_ARGS__)

template<class Object>
using SerDeFactory = PluginFactory<std::shared_ptr<SerDeFunc<Object> > >;

struct AnyPluginManip : public PluginManipFunc<AnyPlugin> {
  void Update(AnyPlugin*, const json&, const json&, const SidePluginRepo&) const final;
  std::string ToString(const AnyPlugin&, const json& dump_options,
                       const SidePluginRepo&) const final;
};
#define ROCKSDB_REG_AnyPluginManip(ClassName) \
  PluginFactory<const PluginManipFunc<AnyPlugin>*>::Reg \
      ROCKSDB_PP_CAT_2(g_reg_manip_any_plugin_,__LINE__) \
     (ClassName, &PluginManipSingleton<AnyPluginManip>, __FILE__, __LINE__)

/// Concrete class defined non-virtual Update() & ToString()
/// EasyProxyManip is a proxy which forward Update() & ToString() to Concrete
template<class Concrete, class Interface>
struct EasyProxyManip : public PluginManipFunc<Interface> {
  void Update(Interface* x, const json& q, const json& j, const SidePluginRepo& r)
  const final {
    assert(dynamic_cast<Concrete*>(x) != nullptr);
    return static_cast<Concrete*>(x)->Update(q, j, r);
  }
  std::string ToString(const Interface& x, const json& dump_options,
                       const SidePluginRepo& r) const final {
    assert(dynamic_cast<const Concrete*>(&x) != nullptr);
    return static_cast<const Concrete&>(x).ToString(dump_options, r);
  }
};
#define ROCKSDB_REG_EasyProxyManip_3(ClassName, ClassType, Interface) \
  PluginFactory<const PluginManipFunc<Interface>*>::Reg \
      ROCKSDB_PP_CAT_3(g_reg_manip_,ClassType,__LINE__) \
     (ClassName, &PluginManipSingleton<EasyProxyManip<ClassType, Interface> >, __FILE__, __LINE__)
#define ROCKSDB_REG_EasyProxyManip_2(ClassType, Interface) \
        ROCKSDB_REG_EasyProxyManip_3(#ClassType, ClassType, Interface)
// call ROCKSDB_REG_EasyProxyManip_${ArgNum}, ArgNum must be 2 or 3
#define ROCKSDB_REG_EasyProxyManip(...) ROCKSDB_PP_CAT2 \
       (ROCKSDB_REG_EasyProxyManip_,ROCKSDB_PP_ARG_N(__VA_ARGS__))(__VA_ARGS__)

std::string PluginParseInstID(const std::string& str_val);

const json& jsonRefType();
const SidePluginRepo& repoRefType();

///@param Name     string of factory class_name
///@param Acquire  must return base class ptr
#define ROCKSDB_FACTORY_REG(Name, Acquire) \
  PluginFactory<decltype(Acquire(jsonRefType(),repoRefType()))>::Reg \
  ROCKSDB_PP_CAT_3(g_reg_factory_,Acquire,__LINE__) \
  (Name,Acquire,__FILE__,__LINE__)

///@param Name       string of factory class_name
///@param Acquire()  must return base class ptr
#define ROCKSDB_FACTORY_REG_0(Name, Acquire) \
  static auto Acquire##_JsonRepo(const json&, const SidePluginRepo&) \
    { return Acquire(); } \
  PluginFactory<decltype(Acquire())>::Reg \
  ROCKSDB_PP_CAT_3(g_reg_factory_,Acquire,__LINE__) \
  (Name,Acquire##_JsonRepo,__FILE__,__LINE__)

template<class ConcretClass, class Interface>
std::shared_ptr<Interface>
JS_NewDefaultConsObject(const json&, const SidePluginRepo&) {
  return std::make_shared<ConcretClass>();
}
template<class ConcretClass, class Interface>
std::shared_ptr<Interface>
JS_NewJsonRepoConsObject(const json& js, const SidePluginRepo& repo) {
  return std::make_shared<ConcretClass>(js, repo);
}
#define ROCKSDB_REG_DEFAULT_CONS_3(Name, ConcretClass, Interface) \
  PluginFactory<std::shared_ptr<Interface> >::Reg \
      ROCKSDB_PP_CAT_3(g_reg_factory_,ConcretClass,__LINE__) \
     (Name,  &JS_NewDefaultConsObject<ConcretClass,Interface>, __FILE__, __LINE__)
#define ROCKSDB_REG_JSON_REPO_CONS_3(Name, ConcretClass, Interface) \
  PluginFactory<std::shared_ptr<Interface> >::Reg \
      ROCKSDB_PP_CAT_3(g_reg_factory_,ConcretClass,__LINE__) \
     (Name, &JS_NewJsonRepoConsObject<ConcretClass,Interface>, __FILE__, __LINE__)

#define ROCKSDB_REG_DEFAULT_CONS_2(ConcretClass, Interface) \
        ROCKSDB_REG_DEFAULT_CONS_3(#ConcretClass, ConcretClass, Interface)
#define ROCKSDB_REG_JSON_REPO_CONS_2(ConcretClass, Interface) \
        ROCKSDB_REG_JSON_REPO_CONS_3(#ConcretClass, ConcretClass, Interface)

// call ROCKSDB_REG_DEFAULT_CONS_${ArgNum}, ArgNum must be 2 or 3
#define ROCKSDB_REG_DEFAULT_CONS(...) ROCKSDB_PP_CAT2 \
       (ROCKSDB_REG_DEFAULT_CONS_,ROCKSDB_PP_ARG_N(__VA_ARGS__))(__VA_ARGS__)
// call ROCKSDB_REG_JSON_REPO_CONS_${ArgNum}, ArgNum must be 2 or 3
#define ROCKSDB_REG_JSON_REPO_CONS(...) ROCKSDB_PP_CAT2 \
       (ROCKSDB_REG_JSON_REPO_CONS_,ROCKSDB_PP_ARG_N(__VA_ARGS__))(__VA_ARGS__)

// ROCKSDB_REG_Plugin auto check default cons or json+repo cons,
// in most cases you should use ROCKSDB_REG_Plugin, if both default cons
// and json+repo cons are defined, explicitly use ROCKSDB_REG_DEFAULT_CONS
// or ROCKSDB_REG_JSON_REPO_CONS
#define ROCKSDB_REG_Plugin_3(Name, ConcretClass, Interface) \
  PluginFactory<std::shared_ptr<Interface> >::Reg \
      ROCKSDB_PP_CAT_3(g_reg_factory_,ConcretClass,__LINE__) \
     (Name,  &JS_NewSidePlugin<ConcretClass,Interface>, __FILE__, __LINE__)

#define ROCKSDB_REG_Plugin_2(ConcretClass, Interface) \
        ROCKSDB_REG_Plugin_3(#ConcretClass, ConcretClass, Interface)

// call ROCKSDB_REG_Plugin_${ArgNum}, ArgNum must be 2 or 3
#define ROCKSDB_REG_Plugin(...) ROCKSDB_PP_CAT2 \
       (ROCKSDB_REG_Plugin_,ROCKSDB_PP_ARG_N(__VA_ARGS__))(__VA_ARGS__)

//////////////////////////////////////////////////////////////////////////////

#define ROCKSDB_JSON_XXX_PROP(js, prop, pname) \
    if (auto __iter = js.find(pname); js.end() != __iter) try { \
      prop = __iter.value().get<std::remove_reference_t<decltype(prop)> >(); \
    } catch (const std::exception& ex) {     \
      THROW_InvalidArgument( \
        "\"" pname "\": js = " + js.dump() + " => " + ex.what()); \
    }

// _REQ_ means 'required'
// _OPT_ means 'optional'
#define ROCKSDB_JSON_REQ_PROP(js, prop) do { \
    ROCKSDB_JSON_XXX_PROP(js, prop, #prop)   \
    else THROW_InvalidArgument(             \
      "missing required param \"" #prop "\", js = " + js.dump()); \
  } while (0)
#define ROCKSDB_JSON_REQ_PROP_3(js, prop, pname) do { \
    ROCKSDB_JSON_XXX_PROP(js, prop, pname)  \
    else THROW_InvalidArgument(             \
      "missing required param \"" pname "\", js = " + js.dump()); \
  } while (0)
#define ROCKSDB_JSON_OPT_PROP(js, prop) do { \
    ROCKSDB_JSON_XXX_PROP(js, prop, #prop)   \
  } while (0)
#define ROCKSDB_JSON_OPT_PROP_3(js, prop, pname) do { \
    ROCKSDB_JSON_XXX_PROP(js, prop, pname)            \
  } while (0)
#define ROCKSDB_JSON_REQ_SIZE(js, prop) prop = JsParseSizeXiB(js, #prop)
#define ROCKSDB_JSON_OPT_SIZE(js, prop) do try { \
      prop = JsParseSizeXiB(js, #prop); \
    } catch (const std::exception&) {} while (0)
#define ROCKSDB_JSON_OPT_ENUM(js, prop) do { \
    if (auto __iter = js.find(#prop); js.end() != __iter) { \
      if (!__iter.value().is_string())       \
        THROW_InvalidArgument("enum \"" #prop "\" must be json string"); \
      const auto& __val = __iter.value().get_ref<const std::string&>(); \
      if (!enum_value(__val, &prop)) \
        THROW_InvalidArgument("bad " #prop " = " + __val); \
  }} while (0)

#define ROCKSDB_JSON_OPT_ESET(js, prop, pname) \
        ROCKSDB_JSON_OPT_ESET_3(js, prop, #prop)
#define ROCKSDB_JSON_OPT_ESET_3(js, prop, pname) do { \
    if (auto __iter = js.find(pname); js.end() != __iter) { \
      if (!__iter.value().is_string())       \
        THROW_InvalidArgument("enum \"" pname "\" must be json string"); \
      const auto& __val = __iter.value().get_ref<const std::string&>(); \
      if (!enum_flags(__val, &prop)) \
        THROW_InvalidArgument("bad " pname " = " + __val); \
  }} while (0)

#define ROCKSDB_JSON_OPT_NEST(js, prop) \
  do try { \
    if (auto __iter = js.find(#prop); js.end() != __iter) \
      prop = decltype(NestForBase(prop))(__iter.value()); \
  } catch (const std::exception& ex) { \
    THROW_InvalidArgument(std::string(#prop ": ") + ex.what()); \
  } while (0)

#define ROCKSDB_JSON_OPT_FACT_INNER(js, prop) \
    prop = PluginFactory<decltype(prop)>:: \
        ObtainPlugin(#prop, ROCKSDB_FUNC, js, repo)
#define ROCKSDB_JSON_OPT_FACT(js, prop) do { \
    if (auto __iter = js.find(#prop); js.end() != __iter) { \
      ROCKSDB_JSON_OPT_FACT_INNER(__iter.value(), prop); \
  }} while (0)

#define ROCKSDB_JSON_SET_SIZE(js, prop) js[#prop] = SizeToString(prop)
#define ROCKSDB_JSON_SET_PROP(js, prop) js[#prop] = prop
#define ROCKSDB_JSON_SET_ENUM(js, prop) js[#prop] = enum_stdstr(prop)
#define ROCKSDB_JSON_SET_NEST(js, prop) \
  static_cast<const decltype(NestForBase(prop))&>(prop).SaveToJson(js[#prop])

/// for which prop and repo_field with different name
#define ROCKSDB_JSON_SET_FACX(js, prop, repo_field) \
        ROCKSDB_JSON_SET_FACT_INNER(js[#prop], prop, repo_field)

/// this Option and repo has same name prop
#define ROCKSDB_JSON_SET_FACT(js, prop) \
        ROCKSDB_JSON_SET_FACT_INNER(js[#prop], prop, prop)

#define ROCKSDB_JSON_SET_FACT_INNER(inner, prop, repo_field) \
  JsonRepoSet(inner, GetRawPtr(prop), \
              repo.m_impl->repo_field.p2name, #repo_field, html)

// Intentional: Interface is first template arg
template<class Interface, class Concret>
bool TemplatePropLoadFromJson(Concret* self, const json& js, const SidePluginRepo& repo) {
  static_assert(std::is_base_of<Interface, Concret>::value);
  if (auto iter = js.find("template"); js.end() != iter) {
    auto name = iter.value().get_ref<const std::string&>().c_str();
    auto tmpl = PluginFactorySP<Interface>::GetPlugin(name, ROCKSDB_FUNC, name, repo);
    if (!tmpl) {
      THROW_InvalidArgument(Slice("not found template name = ") + name);
    }
    if constexpr (std::is_polymorphic<Interface>::value) {
      ROCKSDB_VERIFY(dynamic_cast<const Concret*>(tmpl.get()) != nullptr);
    }
    *self = static_cast<const Concret&>(*tmpl);
    return true;
  }
  return false;
}

template<class Interface, class Concret>
void TemplatePropSaveToJson(json& js, const Concret* self,
              const char* mapname, const char* func,
              const SidePluginRepo::Impl::ObjRepo<Interface>& obj_repo,
              const SidePluginRepo& repo, bool html) {
  static_assert(std::is_base_of<Interface, Concret>::value);
  if (const json* spec = repo.GetCreationSpec(self)) {
    auto& param_js = (*spec)["params"];
    if (auto tji = param_js.find("template"); param_js.end() != tji) {
      const json& namej = tji.value();
      auto name = namej.get_ref<const std::string&>().c_str();
      auto tmpl = PluginFactorySP<Interface>::GetPlugin(name, func, namej, repo);
      ROCKSDB_VERIFY(tmpl != nullptr);
      JsonRepoSet(js["template"], tmpl.get(), obj_repo.p2name, mapname, html);
    }
  }
}
#define ROCKSDB_JSON_SET_TMPL(js, mapname) TemplatePropSaveToJson \
  (js, this, #mapname, ROCKSDB_FUNC, repo.m_impl->mapname, repo, html)

bool SameVarName(const std::string&, const std::string&);

std::string
JsonRepoGetHtml_ahref(const char* mapname, const std::string& varname);
void
JsonRepoSetHtml_ahref(json&, const char* mapname, const std::string& varname);

void JsonRepoSet(json& js, const void* prop,
                 const std::map<const void*, SidePluginRepo::Impl::ObjInfo>&,
                 const char* mapname, bool html);

std::string SidePluginHyperLink(const void* obj,
                 const std::map<const void*, SidePluginRepo::Impl::ObjInfo>&,
                 const char* mapname, const char* link_text, bool html);

std::string Json_DB_CF_SST_HtmlTable(class Version*, class ColumnFamilyData*);
std::string Json_DB_CF_SST_HtmlTable(class Version*, class ColumnFamilyData*,
                                     struct TableProperties* all_agg);

struct CompactionParams;
json JS_CompactionParamsEncodePtr(const CompactionParams*);
const CompactionParams* JS_CompactionParamsDecodePtr(const json&);

void JS_ToplingDB_AddVersion(json& djs, bool html);
void JS_TopTable_AddVersion(json& djs, bool html); // in repo topling-sst
void JS_ModuleGitInfo_Add(json& djs, bool html);

json DbPathVecToJson(const std::vector<struct DbPath>&, bool html);
json TableUserPropsToJson(const std::map<std::string, std::string>&, const json& dump_options);

} // ROCKSDB_NAMESPACE
