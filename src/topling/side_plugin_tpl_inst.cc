#include "side_plugin_factory.h"

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
#include "logging/logging.h"
#include "util/string_util.h"

namespace ROCKSDB_NAMESPACE {

template<class Ptr>
struct PluginFactory<Ptr>::Reg::Impl {
  Impl(const Impl&) = delete;
  Impl& operator=(const Impl&) = delete;
  Impl() = default;
  NameToFuncMap func_map;
  std::map<std::string, Ptr> inst_map;
  static Impl& s_singleton();
};

template<class Ptr>
typename
PluginFactory<Ptr>::Reg::Impl&
PluginFactory<Ptr>::Reg::Impl::s_singleton() { static Impl imp; return imp; }

template<class Ptr>
PluginFactory<Ptr>::Reg::Reg(Slice class_name, AcqFunc acq,
                             const char* file, int line, Slice base_class)
noexcept {
  auto& imp = Impl::s_singleton();
  Meta meta{acq, base_class.ToString()};
  auto ib = imp.func_map.insert({class_name.ToString(), std::move(meta)});
  if (!ib.second) {
    fprintf(stderr,
            "%s: FATAL: %s:%d: PluginFactory<%s>::Reg: dup class = %s\n",
            StrDateTimeNow(), RocksLogShorterFileName(file), line,
            demangle(typeid(Ptr)).c_str(), class_name.data());
    abort();
  }
  if (SidePluginRepo::DebugLevel() >= 2) {
    fprintf(stderr, "%s: INFO: %s:%d: PluginFactory<%s>::Reg: class = %s\n",
            StrDateTimeNow(), RocksLogShorterFileName(file), line,
            demangle(typeid(Ptr)).c_str(), class_name.data());
  }
  this->ipos = ib.first;
}

template<class Ptr>
PluginFactory<Ptr>::Reg::~Reg() {
  // static const char* env = getenv("SidePlugin_unknown_double_free");
  // static const bool  val = env ? bool(atoi(env)) : false;
  // if (val) return; // skip erase
  auto& imp = Impl::s_singleton();
  imp.func_map.erase(ipos);
}

template<class Ptr>
Ptr
PluginFactory<Ptr>::AcquirePlugin(const std::string& clazz, const json& js,
                                  const SidePluginRepo& repo) {
  auto& imp = Reg::Impl::s_singleton();
  auto iter = imp.func_map.find(clazz);
  if (imp.func_map.end() != iter) {
    Ptr ptr = iter->second.acq(js, repo);
    assert(!!ptr);
    return ptr;
  }
  else {
    //return Ptr(nullptr);
    THROW_NotFound("class = " + clazz + ", params = " + js.dump());
  }
}

template<class Ptr>
Ptr
PluginFactory<Ptr>::NullablePlugin(const std::string& clazz, const json& js,
                                   const SidePluginRepo& repo) {
  auto& imp = Reg::Impl::s_singleton();
  auto iter = imp.func_map.find(clazz);
  if (imp.func_map.end() != iter) {
    Ptr ptr = iter->second.acq(js, repo);
    assert(!!ptr);
    return ptr;
  }
  return Ptr(nullptr);
}

#define do_not_instantiate(Func, Ptr) \
static void \
Func(const char* varname, const char* func_name, \
     const json& js, const SidePluginRepo& repo, Ptr* pp) { \
  TERARK_DIE("Should not goes here"); \
}

do_not_instantiate(GetNoAcq, std::shared_ptr<CFPropertiesWebView>)
do_not_instantiate(GetOrAcq, std::shared_ptr<CFPropertiesWebView>)
do_not_instantiate(GetNoAcq, std::shared_ptr<TableReader>)
do_not_instantiate(GetOrAcq, std::shared_ptr<TableReader>)
template<class T> do_not_instantiate(GetNoAcq, const PluginManipFunc<T>*)
template<class T> do_not_instantiate(GetOrAcq, const PluginManipFunc<T>*)
template<class T> do_not_instantiate(GetNoAcq, std::shared_ptr<SerDeFunc<T> >)
template<class T> do_not_instantiate(GetOrAcq, std::shared_ptr<SerDeFunc<T> >)

template<class Ptr>
static void
GetNoAcq(const char* varname, const char* func_name,
         const json& js, const SidePluginRepo& repo, Ptr* pp) {
  if (js.is_string()) {
    const auto& str_val = js.get_ref<const std::string&>();
    if (str_val.empty()) {
      throw Status::InvalidArgument(
          func_name, std::string(varname) + " inst_id/class_name is empty");
    }
    bool ret = false;
    if ('$' == str_val[0]) {
      if (str_val.size() < 3) {
        throw Status::InvalidArgument(func_name,
                   std::string(varname) + " inst_id is too short");
      }
      const auto inst_id = PluginParseInstID(str_val);
      ret = repo.Get(inst_id, pp);
    } else {
      ret = repo.Get(str_val, pp); // the whole str_val is inst_id
    }
    if (!ret) {
      throw Status::NotFound(func_name,
            std::string(varname) + "inst_id = " + str_val);
    }
    assert(!!*pp);
  }
  else {
    throw Status::InvalidArgument(func_name,
      std::string(varname) + " must be a string for reference to object");
  }
}

///@param varname just for error report
///@param func_name just for error report
//
// if json is a string ${inst_id} or $inst_id, then Get the plugin named
// inst_id in repo.
//
// if json is a string does not like ${inst_id} or $inst_id, then the string
// is treated as a class name to create the plugin with empty json params.
//
// if json is an object, it should be { class: class_name, params: ... }
template<class Ptr>
static void 
GetOrAcq(const char* varname, const char* func_name,
         const json& js, const SidePluginRepo& repo, Ptr* pp) {
  if (js.is_string()) {
    const auto& str_val = js.get_ref<const std::string&>();
    if (str_val.empty()) {
    #if 1
      // treat empty string json as null, because json convert to yaml
      // may convert json null into empty string, convert such yaml back
      // to json yield an empty string ""
    #else
      throw Status::InvalidArgument(func_name, std::string(varname) +
               " inst_id/class_name is empty");
    #endif
    }
    else if ('$' == str_val[0]) {
      if (str_val.size() < 3) {
        throw Status::InvalidArgument(func_name, std::string(varname) +
                 " inst_id = \"" + str_val + "\" is too short");
      }
      const auto inst_id = PluginParseInstID(str_val);
      if (!repo.Get(inst_id, pp)) {
        throw Status::NotFound(func_name,
           std::string(varname) + " inst_id = \"" + inst_id + "\"");
      }
      assert(!!*pp);
    } else {
      // string which does not like ${inst_id} or $inst_id
      // try to treat str_val as inst_id to Get it
      if (repo.Get(str_val, pp)) {
        assert(!!*pp);
      }
      else {
        // now treat str_val as class name, try to --
        // AcquirePlugin with empty json params
        const std::string& clazz_name = str_val;
        *pp = PluginFactory<Ptr>::AcquirePlugin(clazz_name, json{}, repo);
      }
    }
  } else if (js.is_null()) {
    // do nothing
  } else if (js.is_object()) {
    auto iter = js.find("class");
    if (js.end() == iter) {
        throw Status::InvalidArgument(func_name, "sub obj class is required");
    }
    if (!iter.value().is_string()) {
        throw Status::InvalidArgument(func_name, "sub obj class must be string");
    }
    const auto& clazz_name = iter.value().get_ref<const std::string&>();
    const json& params = js.at("params");
    *pp = PluginFactory<Ptr>::AcquirePlugin(clazz_name, params, repo);
  }
  else {
    throw Status::InvalidArgument(func_name,
        "js must be string, null, or object, but is: " + js.dump());
  }
}

template<class Ptr>
Ptr
PluginFactory<Ptr>::
GetPlugin(const char* varname, const char* func_name,
          const json& js, const SidePluginRepo& repo) {
  Ptr p(nullptr);
  GetNoAcq(varname, func_name, js, repo, &p);
  return p;
}

template<class Ptr>
Ptr
PluginFactory<Ptr>::
ObtainPlugin(const char* varname, const char* func_name,
             const json& js, const SidePluginRepo& repo) {
  Ptr p(nullptr);
  GetOrAcq(varname, func_name, js, repo, &p);
  return p;
}

template<class Ptr>
Ptr
PluginFactory<Ptr>::
AcquirePlugin(const json& js, const SidePluginRepo& repo) {
  if (js.is_string()) {
    const auto& str_val = js.get_ref<const std::string&>();
    if (str_val.empty()) {
      THROW_InvalidArgument("jstr class_name is empty");
    }
    // now treat js as class name, try to --
    // AcquirePlugin with empty json params
    const std::string& clazz_name = str_val;
    return AcquirePlugin(clazz_name, json{}, repo);
  } else if (js.is_null()) {
    return Ptr(nullptr);
  } else if (js.is_object()) {
    auto iter = js.find("class");
    if (js.end() == iter) {
      THROW_InvalidArgument("js[\"class\"] is required: " + js.dump());
    }
    if (!iter.value().is_string()) {
      THROW_InvalidArgument("js[\"class\"] must be string: " + js.dump());
    }
    const auto& clazz_name = iter.value().get_ref<const std::string&>();
    const json& params = js.at("params");
    return AcquirePlugin(clazz_name, params, repo);
  }
  THROW_InvalidArgument(
        "js must be string, null, or object, but is: " + js.dump());
}

template<class Ptr>
bool PluginFactory<Ptr>::HasPlugin(const std::string& class_name) {
  auto& imp = Reg::Impl::s_singleton();
  return imp.func_map.count(class_name) != 0;
}

// plugin can have alias class name, this function check whether the two
// aliases are defined as a same plugin
template<class Ptr>
bool PluginFactory<Ptr>::SamePlugin(const std::string& clazz1,
                                    const std::string& clazz2) {
  if (clazz1 == clazz2) {
    return true;
  }
  auto& imp = Reg::Impl::s_singleton();
  auto i1 = imp.func_map.find(clazz1);
  auto i2 = imp.func_map.find(clazz2);
  if (imp.func_map.end() == i1) {
    THROW_NotFound("clazz1 = " + clazz1);
  }
  if (imp.func_map.end() == i2) {
    THROW_NotFound("clazz2 = " + clazz2);
  }
  return i1->second.acq == i2->second.acq;
}

#define explicit_instantiate_sp(Interface) \
  template class PluginFactory<std::shared_ptr<Interface> >; \
  template class PluginFactory<const PluginManipFunc<Interface>*>; \
  template class PluginFactory<std::shared_ptr<SerDeFunc<std::remove_const_t<Interface> > > >

#define explicit_instantiate_rp(Interface) \
  template class PluginFactory<Interface*>; \
  template class PluginFactory<const PluginManipFunc<Interface>*>; \
  template class PluginFactory<std::shared_ptr<SerDeFunc<std::remove_const_t<Interface> > > >

explicit_instantiate_sp(AnyPlugin);
explicit_instantiate_sp(Cache);
explicit_instantiate_sp(PersistentCache);
explicit_instantiate_sp(CompactionExecutorFactory);
explicit_instantiate_sp(CompactionFilterFactory);
explicit_instantiate_sp(ConcurrentTaskLimiter);
explicit_instantiate_sp(EventListener);
explicit_instantiate_sp(FileChecksumGenFactory);
explicit_instantiate_sp(FileSystem);
explicit_instantiate_sp(const FilterPolicy);
explicit_instantiate_sp(FlushBlockPolicyFactory);
explicit_instantiate_sp(Logger);
explicit_instantiate_sp(MemoryAllocator);
explicit_instantiate_sp(MemTableRepFactory);
explicit_instantiate_sp(MergeOperator);
explicit_instantiate_sp(RateLimiter);
explicit_instantiate_sp(SstFileManager);
explicit_instantiate_sp(SstPartitionerFactory);
explicit_instantiate_sp(Statistics);
explicit_instantiate_sp(TableFactory);
explicit_instantiate_sp(TablePropertiesCollectorFactory);
explicit_instantiate_sp(TransactionDBMutexFactory);
explicit_instantiate_sp(WriteBufferManager);
explicit_instantiate_sp(const SliceTransform);

explicit_instantiate_sp(Options);
explicit_instantiate_sp(DBOptions);
explicit_instantiate_sp(ColumnFamilyOptions);

explicit_instantiate_sp(CFPropertiesWebView);

explicit_instantiate_sp(TableReader);

explicit_instantiate_rp(const Comparator);
explicit_instantiate_rp(Env);

explicit_instantiate_rp(DB_MultiCF);
explicit_instantiate_rp(DB);

} // ROCKSDB_NAMESPACE
