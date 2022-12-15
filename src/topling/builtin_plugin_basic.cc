#include "side_plugin_factory.h"

#include <rocksdb/flush_block_policy.h>
#include <rocksdb/memory_allocator.h>
#include <rocksdb/memtablerep.h>
#include <rocksdb/rate_limiter.h>
#include <rocksdb/slice_transform.h>
#include <db/compaction/compaction_executor.h>

#include <cache/lru_cache.h>

#include <util/coding_lean.h>
#include <util/rate_limiter.h>

#include <utilities/table_properties_collectors/compact_on_deletion_collector.h>

#if defined(MEMKIND)
  #include "memory/memkind_kmem_allocator.h"
#endif

const char* git_version_hash_info_cspp_memtable();
const char* git_version_hash_info_cspp_wbwi();

namespace ROCKSDB_NAMESPACE {

using std::shared_ptr;
using std::string;


AnyPlugin::~AnyPlugin() = default;

std::string
CompactionParams_html_user_key_decode(const CompactionParams& cp, Slice uk) {
  if (cp.p_html_user_key_coder)
    return cp.p_html_user_key_coder->Decode(uk);
  else
    return uk.ToString(true); // hex
}

void HtmlAppendEscapeMin(std::string* d, const char* s, size_t n) {
  for (size_t i = 0; i < n; ++i) {
    const char c = s[i];
    switch (c) {
      default : d->push_back(c);    break;
      case  0 : d->append("<em>\\0</em>");   break;
      case '<': d->append("&lt;" ); break;
      case '>': d->append("&gt;" ); break;
      case '&': d->append("&amp;"); break;
    }
  }
}

void HtmlAppendEscape(std::string* d, const char* s, size_t n) {
  for (size_t i = 0; i < n; ++i) {
    const char c = s[i];
    switch (c) {
      default : d->push_back(c);    break;
      case  0 : d->append("<em>\\0</em>");   break;
      case  1 : d->append("<em>\\1</em>");   break;
      case  2 : d->append("<em>\\2</em>");   break;
      case  3 : d->append("<em>\\3</em>");   break;
      case  4 : d->append("<em>\\4</em>");   break;
      case  5 : d->append("<em>\\5</em>");   break;
      case  6 : d->append("<em>\\6</em>");   break;
      case  7 : d->append("<em>\\7</em>");   break;
      case '\\': d->append("<em>\\\\</em>"); break;
      case '\b': d->append("<em>\\b</em>");  break;
      case '\t': d->append("<em>\\t</em>");  break;
      case '\r': d->append("<em>\\r</em>");  break;
      case '\n': d->append("<em>\\n</em>");  break;
      case '<': d->append("&lt;" ); break;
      case '>': d->append("&gt;" ); break;
      case '&': d->append("&amp;"); break;
    }
  }
}
struct HtmlTextUserKeyCoder : public UserKeyCoder {
  const char* Name() const override { return "HtmlTextUserKeyCoder"; }
  void Update(const json&, const json&, const SidePluginRepo&) override {
    ROCKSDB_DIE("This function should not be called");
  }
  std::string ToString(const json&, const SidePluginRepo&) const override {
    return R"(<h3>this is HtmlTextUserKeyCoder</h3>
<p>1. convert <code>&amp; &lt; &gt;</code> to <code>&amp;amp; &amp;lt; &amp;gt;</code></p>
<p>2. also escape chars as C string(such as \t, \r, \n)</p>
)";
  }
  void Encode(Slice, std::string*) const override {
    ROCKSDB_DIE("This function should not be called");
  }
  void Decode(Slice coded, std::string* de) const override {
    de->clear();
    de->reserve(coded.size_);
    HtmlAppendEscape(de, coded.data_, coded.size_);
  }
};
std::shared_ptr<AnyPlugin>
JS_NewHtmlTextUserKeyCoder(const json&, const SidePluginRepo&) {
  static const auto single = std::make_shared<HtmlTextUserKeyCoder>();
  return single;
}
ROCKSDB_FACTORY_REG("HtmlTextUserKeyCoder", JS_NewHtmlTextUserKeyCoder);
ROCKSDB_REG_AnyPluginManip("HtmlTextUserKeyCoder");

struct HexUserKeyCoder : public UserKeyCoder {
  size_t prefix_len = 0;
  HexUserKeyCoder(const json& js, const SidePluginRepo& repo) {
    Update({}, js, repo);
  }
  const char* Name() const override { return "HexUserKeyCoder"; }
  void Update(const json&, const json& js, const SidePluginRepo&) override {
    ROCKSDB_JSON_OPT_PROP(js, prefix_len);
    if (prefix_len > 8)
      THROW_InvalidArgument("prefix_len must <= 8");
  }
  std::string ToString(const json& dump_options, const SidePluginRepo&)
  const override {
    json js;
    ROCKSDB_JSON_SET_PROP(js, prefix_len);
    return JsonToString(js, dump_options);
  }
  void Encode(Slice, std::string*) const override {
    ROCKSDB_DIE("This function should not be called");
  }
  static uint64_t ReadBigEndianUint64(const void* beg, size_t len) {
    union {
      unsigned char bytes[8];
      uint64_t value;
    } c;
    c.value = 0;  // this is fix for gcc-4.8 union init bug
    memcpy(c.bytes + (8 - len), beg, len);
    if (port::kLittleEndian)
      return __bswap_64(c.value);
    else
      return c.value;
  }
  void Decode(Slice coded, std::string* de) const override {
    if (prefix_len) {
      de->clear();
      de->reserve(coded.size_*2 + 32);
      if (coded.size() >= prefix_len) {
        uint64_t prefix = ReadBigEndianUint64(coded.data(), prefix_len);
        de->append("<b style='color:green'>");
        de->append(std::to_string(prefix));
        de->append("</b>:");
        coded.remove_prefix(prefix_len);
        de->append(coded.ToString(true));
      } else {
        de->append("<b style='color:red'>");
        de->append(coded.ToString(true));
        de->append("</b>");
      }
    } else {
      *de = coded.ToString(true);
    }
  }
};
ROCKSDB_REG_Plugin(HexUserKeyCoder, AnyPlugin);
ROCKSDB_REG_AnyPluginManip("HexUserKeyCoder");

struct DbBenchUserKeyCoder : public UserKeyCoder {
  int prefix_len = 0;
  int key_size = 16;
  DbBenchUserKeyCoder(const json& js, const SidePluginRepo&) {
    ROCKSDB_JSON_OPT_PROP(js, prefix_len);
    ROCKSDB_JSON_OPT_PROP(js, key_size); // >= prefix_len + 8
    prefix_len = std::max(prefix_len, 0);
    key_size = std::max(key_size, prefix_len + 8);
  }
  const char* Name() const override { return "DbBenchUserKeyCoder"; }
  void Update(const json&, const json&, const SidePluginRepo&) override {
    ROCKSDB_DIE("This function should not be called");
  }
  std::string ToString(const json&, const SidePluginRepo&) const override {
    char buf[1024];
    auto len = snprintf(buf, sizeof(buf), R"(<h3>this is DbBenchUserKeyCoder</h3>
<h4>prefix_len = %d, key_len = %d</h4>
<p>Generate key according to the given specification and random number.</p>
<p>The resulting key will have the following format:</p>
<pre>
  - If keys_per_prefix_ is positive, extra trailing bytes are either cut
    off or padded with '0'.
    The prefix value is derived from key value.
    ----------------------------
    | prefix 00000 | key 00000 |
    ----------------------------
</pre>
<p></p>
<pre>
  - If keys_per_prefix_ is 0, the key is simply a binary representation of
    random number followed by trailing '0's
    ----------------------------
    |        key 00000         |
    ----------------------------
</pre>
)", prefix_len, key_size);
    return std::string(buf, len);
  }
  void Encode(Slice, std::string*) const override {
    ROCKSDB_DIE("This function should not be called");
  }
  void Decode(Slice coded, std::string* de) const override {
    de->clear();
    de->reserve(key_size * 2 + 32);
    long long prefix, keyint;
    memcpy(&prefix, coded.data_, 8);
    memcpy(&keyint, coded.data_ + prefix_len, 8);
    if (port::kLittleEndian) {
      prefix = EndianSwapValue(prefix);
      keyint = EndianSwapValue(keyint);
    }
    char buf[24];
    if (prefix_len) {
      sprintf(buf, "%016llX", prefix);
      de->append("<code>");
      de->append(buf, 16);
      de->append("</code>:<code>");
    }
    else {
      de->append("<code>");
    }
    sprintf(buf, "%016llX", keyint);
    de->append(buf, 16);
    de->append("</code>");
  }
};
ROCKSDB_REG_Plugin(DbBenchUserKeyCoder, AnyPlugin);
ROCKSDB_REG_AnyPluginManip("DbBenchUserKeyCoder");

__attribute__((weak)) void JS_ZipTable_AddVersion(json& djs, bool html);

void JS_CSPPMemTab_AddVersion(json& djs, bool html);
void JS_CSPP_WBWI_AddVersion(json& djs, bool html);

void JS_ModuleGitInfo_Add(json& js, bool html) {
  JS_ToplingDB_AddVersion(js, html);
  JS_CSPPMemTab_AddVersion(js, html);
  JS_CSPP_WBWI_AddVersion(js, html);
  if (JS_ZipTable_AddVersion)
    JS_ZipTable_AddVersion(js, html);
  JS_TopTable_AddVersion(js, html);
}
class ModuleGitInfo : public AnyPlugin {
public:
  const char* Name() const final { return "ModuleGitInfo"; }
  void Update(const json&, const json&, const SidePluginRepo&) override {}
  std::string ToString(const json& dump_options, const SidePluginRepo&)
  const override {
    bool html = JsonSmartBool(dump_options, "html", true);
    json js;
    JS_ModuleGitInfo_Add(js, html);
    return JsonToString(js, dump_options);
  }
};
ROCKSDB_REG_DEFAULT_CONS(ModuleGitInfo, AnyPlugin);
ROCKSDB_REG_AnyPluginManip("ModuleGitInfo");

struct LRUCacheOptions_Json : LRUCacheOptions {
  LRUCacheOptions_Json(const json& js, const SidePluginRepo& repo) {
    ROCKSDB_JSON_REQ_SIZE(js, capacity);
    ROCKSDB_JSON_OPT_PROP(js, num_shard_bits);
    ROCKSDB_JSON_OPT_PROP(js, strict_capacity_limit);
    ROCKSDB_JSON_OPT_PROP(js, high_pri_pool_ratio);
    ROCKSDB_JSON_OPT_FACT(js, memory_allocator);
    ROCKSDB_JSON_OPT_PROP(js, use_adaptive_mutex);
    ROCKSDB_JSON_OPT_ENUM(js, metadata_charge_policy);
  }
  json ToJson(const SidePluginRepo& repo, bool html) const {
    json js;
    ROCKSDB_JSON_SET_SIZE(js, capacity);
    ROCKSDB_JSON_SET_PROP(js, num_shard_bits);
    ROCKSDB_JSON_SET_PROP(js, strict_capacity_limit);
    ROCKSDB_JSON_SET_PROP(js, high_pri_pool_ratio);
    ROCKSDB_JSON_SET_FACT(js, memory_allocator);
    ROCKSDB_JSON_SET_PROP(js, use_adaptive_mutex);
    ROCKSDB_JSON_SET_ENUM(js, metadata_charge_policy);
    return js;
  }
};
static std::shared_ptr<Cache>
JS_NewLRUCache(const json& js, const SidePluginRepo& repo) {
  return NewLRUCache(LRUCacheOptions_Json(js, repo));
}
ROCKSDB_FACTORY_REG("LRUCache", JS_NewLRUCache);

struct LRUCache_Manip : PluginManipFunc<Cache> {
  void Update(Cache* cache, const json& query, const json& js, const SidePluginRepo&)
  const override {
    if (js.contains("capacity")) {
      size_t capacity = 0;
      ROCKSDB_JSON_OPT_SIZE(js, capacity);
      cache->SetCapacity(capacity);
    }
    if (js.contains("strict_capacity")) {
      bool strict_capacity = false;
      ROCKSDB_JSON_OPT_PROP(js, strict_capacity);
      cache->SetStrictCapacityLimit(strict_capacity);
    }
    if (query.contains("EraseUnRefEntries")) {
      cache->EraseUnRefEntries();
    }
  }

  string ToString(const Cache& r, const json& dump_options, const SidePluginRepo& repo)
  const override {
    bool html = JsonSmartBool(dump_options, "html", true);
    auto& p2name = repo.m_impl->cache.p2name;
    json js;
    if (auto iter = p2name.find((Cache*)&r); p2name.end() != iter) {
      js = iter->second.params;
    }
    size_t usage = r.GetUsage();
    size_t pined_usage = r.GetPinnedUsage();
    size_t capacity = r.GetCapacity();
    bool strict_capacity = r.HasStrictCapacityLimit();
    double usage_rate = 1.0*usage / capacity;
    double pined_rate = 1.0*pined_usage / capacity;
    MemoryAllocator* memory_allocator = r.memory_allocator();
    ROCKSDB_JSON_SET_SIZE(js, usage);
    ROCKSDB_JSON_SET_SIZE(js, pined_usage);
    ROCKSDB_JSON_SET_SIZE(js, capacity);
    ROCKSDB_JSON_SET_PROP(js, strict_capacity);
    ROCKSDB_JSON_SET_PROP(js, usage_rate);
    ROCKSDB_JSON_SET_PROP(js, pined_rate);
    ROCKSDB_JSON_SET_FACT(js, memory_allocator);
    auto lru = dynamic_cast<const LRUCache*>(&r);
    if (lru) {
      size_t cached_elem_num = const_cast<LRUCache*>(lru)->TEST_GetLRUSize();
      ROCKSDB_JSON_SET_PROP(js, cached_elem_num);
    }
    return JsonToString(js, dump_options);
  }
};
ROCKSDB_REG_PluginManip("LRUCache", LRUCache_Manip);

//////////////////////////////////////////////////////////////////////////////
static std::shared_ptr<Cache>
JS_NewClockCache(const json& js, const SidePluginRepo& repo) {
#ifdef SUPPORT_CLOCK_CACHE
  LRUCacheOptions_Json opt(js, repo); // similar with ClockCache param
  auto p = NewClockCache(opt.capacity, opt.num_shard_bits,
                         opt.strict_capacity_limit, opt.metadata_charge_policy);
  if (nullptr != p) {
	THROW_InvalidArgument(
		"SUPPORT_CLOCK_CACHE is defined but NewClockCache returns null");
  }
  return p;
#else
  (void)js;
  (void)repo;
  THROW_InvalidArgument(
      "SUPPORT_CLOCK_CACHE is not defined, "
      "need to recompile with -D SUPPORT_CLOCK_CACHE=1");
#endif
}
ROCKSDB_FACTORY_REG("ClockCache", JS_NewClockCache);

//////////////////////////////////////////////////////////////////////////////
static std::shared_ptr<const SliceTransform>
JS_NewFixedPrefixTransform(const json& js, const SidePluginRepo&) {
  size_t prefix_len = 0;
  ROCKSDB_JSON_REQ_PROP(js, prefix_len);
  return std::shared_ptr<const SliceTransform>(
      NewFixedPrefixTransform(prefix_len));
}
ROCKSDB_FACTORY_REG("FixedPrefixTransform", JS_NewFixedPrefixTransform);

//////////////////////////////////////////////////////////////////////////////
static std::shared_ptr<const SliceTransform>
JS_NewCappedPrefixTransform(const json& js, const SidePluginRepo&) {
  size_t cap_len = 0;
  ROCKSDB_JSON_REQ_PROP(js, cap_len);
  return std::shared_ptr<const SliceTransform>(
      NewCappedPrefixTransform(cap_len));
}
ROCKSDB_FACTORY_REG("CappedPrefixTransform", JS_NewCappedPrefixTransform);

//////////////////////////////////////////////////////////////////////////////

static const Comparator*
BytewiseComp(const json&, const SidePluginRepo&) {
  return BytewiseComparator();
}
static const Comparator*
RevBytewiseComp(const json&, const SidePluginRepo&) {
  return ReverseBytewiseComparator();
}
ROCKSDB_FACTORY_REG(                   "default", BytewiseComp);
ROCKSDB_FACTORY_REG(                   "Default", BytewiseComp);
ROCKSDB_FACTORY_REG(                  "bytewise", BytewiseComp);
ROCKSDB_FACTORY_REG(                  "Bytewise", BytewiseComp);
ROCKSDB_FACTORY_REG(        "BytewiseComparator", BytewiseComp);
ROCKSDB_FACTORY_REG("leveldb.BytewiseComparator", BytewiseComp);
ROCKSDB_FACTORY_REG("rocksdb.BytewiseComparator", BytewiseComp);
ROCKSDB_FACTORY_REG(        "ReverseBytewise"          , RevBytewiseComp);
ROCKSDB_FACTORY_REG(        "ReverseBytewiseComparator", RevBytewiseComp);
ROCKSDB_FACTORY_REG("rocksdb.ReverseBytewiseComparator", RevBytewiseComp);

//////////////////////////////////////////////////////////////////////////////
static Env* DefaultEnv(const json&, const SidePluginRepo&) {
  return Env::Default();
}
ROCKSDB_FACTORY_REG("default", DefaultEnv);

/////////////////////////////////////////////////////////////////////////////
ROCKSDB_REG_Plugin(FlushBlockBySizePolicyFactory, FlushBlockPolicyFactory);

/////////////////////////////////////////////////////////////////////////////
static shared_ptr<FileChecksumGenFactory>
GetFileChecksumGenCrc32cFactoryJson(const json&,
                                    const SidePluginRepo&) {
  return GetFileChecksumGenCrc32cFactory();
}
ROCKSDB_FACTORY_REG("Crc32c", GetFileChecksumGenCrc32cFactoryJson);
ROCKSDB_FACTORY_REG("crc32c", GetFileChecksumGenCrc32cFactoryJson);

/////////////////////////////////////////////////////////////////////////////

struct JemallocAllocatorOptions_Json : JemallocAllocatorOptions {
  JemallocAllocatorOptions_Json(const json& js, const SidePluginRepo&) {
    ROCKSDB_JSON_OPT_PROP(js, limit_tcache_size);
    ROCKSDB_JSON_OPT_SIZE(js, tcache_size_lower_bound);
    ROCKSDB_JSON_OPT_SIZE(js, tcache_size_upper_bound);
  }
  json ToJson(const SidePluginRepo&) {
    json js;
    ROCKSDB_JSON_SET_PROP(js, limit_tcache_size);
    ROCKSDB_JSON_SET_SIZE(js, tcache_size_lower_bound);
    ROCKSDB_JSON_SET_SIZE(js, tcache_size_upper_bound);
    return js;
  }
};
std::shared_ptr<MemoryAllocator>
JS_NewJemallocNodumpAllocator(const json& js, const SidePluginRepo& repo) {
  JemallocAllocatorOptions_Json opt(js, repo);
  std::shared_ptr<MemoryAllocator> p;
  Status s = NewJemallocNodumpAllocator(opt, &p);
  if (!s.ok()) {
    throw s;
  }
  return p;
}
ROCKSDB_FACTORY_REG("JemallocNodumpAllocator", JS_NewJemallocNodumpAllocator);
#if defined(MEMKIND)
ROCKSDB_REG_Plugin(MemkindKmemAllocator, MemoryAllocator);
#endif

/////////////////////////////////////////////////////////////////////////////
static shared_ptr<MemTableRepFactory>
NewSkipListMemTableRepFactoryJson(const json& js, const SidePluginRepo&) {
  size_t lookahead = 0;
  ROCKSDB_JSON_OPT_PROP(js, lookahead);
  return std::make_shared<SkipListFactory>(lookahead);
}
ROCKSDB_FACTORY_REG("SkipListRep", NewSkipListMemTableRepFactoryJson);
ROCKSDB_FACTORY_REG("SkipList", NewSkipListMemTableRepFactoryJson);
ROCKSDB_FACTORY_REG("skiplist", NewSkipListMemTableRepFactoryJson);

static shared_ptr<MemTableRepFactory>
NewVectorMemTableRepFactoryJson(const json& js, const SidePluginRepo&) {
  size_t count = 0;
  ROCKSDB_JSON_OPT_PROP(js, count);
  return std::make_shared<VectorRepFactory>(count);
}
ROCKSDB_FACTORY_REG("VectorRep", NewVectorMemTableRepFactoryJson);
ROCKSDB_FACTORY_REG("Vector", NewVectorMemTableRepFactoryJson);
ROCKSDB_FACTORY_REG("vector", NewVectorMemTableRepFactoryJson);

static shared_ptr<MemTableRepFactory>
NewHashSkipListMemTableRepFactoryJson(const json& js, const SidePluginRepo&) {
  size_t bucket_count = 1000000;
  int32_t height = 4;
  int32_t branching_factor = 4;
  ROCKSDB_JSON_OPT_PROP(js, bucket_count);
  ROCKSDB_JSON_OPT_PROP(js, height);
  ROCKSDB_JSON_OPT_PROP(js, branching_factor);
  return shared_ptr<MemTableRepFactory>(
      NewHashSkipListRepFactory(bucket_count, height, branching_factor));
}
ROCKSDB_FACTORY_REG("HashSkipListRep", NewHashSkipListMemTableRepFactoryJson);
ROCKSDB_FACTORY_REG("HashSkipList", NewHashSkipListMemTableRepFactoryJson);

static shared_ptr<MemTableRepFactory>
NewHashLinkListMemTableRepFactoryJson(const json& js, const SidePluginRepo&) {
  size_t bucket_count = 50000;
  size_t huge_page_tlb_size = 0;
  int bucket_entries_logging_threshold = 4096;
  bool if_log_bucket_dist_when_flash = true;
  uint32_t threshold_use_skiplist = 256;
  ROCKSDB_JSON_OPT_PROP(js, bucket_count);
  ROCKSDB_JSON_OPT_SIZE(js, huge_page_tlb_size);
  ROCKSDB_JSON_OPT_PROP(js, bucket_entries_logging_threshold);
  ROCKSDB_JSON_OPT_PROP(js, if_log_bucket_dist_when_flash);
  ROCKSDB_JSON_OPT_PROP(js, threshold_use_skiplist);
  return shared_ptr<MemTableRepFactory>(
      NewHashLinkListRepFactory(bucket_count,
                                huge_page_tlb_size,
                                bucket_entries_logging_threshold,
                                if_log_bucket_dist_when_flash,
                                threshold_use_skiplist));
}
ROCKSDB_FACTORY_REG("HashLinkListRep", NewHashLinkListMemTableRepFactoryJson);
ROCKSDB_FACTORY_REG("HashLinkList", NewHashLinkListMemTableRepFactoryJson);

struct DynaMemTableFactory : public MemTableRepFactory {
  shared_ptr<MemTableRepFactory> real_fac = nullptr;
  std::string orig_name;
  std::mutex  m_mtx;
  auto Get_real_fac_iter(const json& js, const SidePluginRepo& repo) {
    auto iter = js.find("real_fac");
    if (js.end() == iter) {
      THROW_InvalidArgument("missing required field 'real_fac'");
    }
    if (!iter.value().is_string()) {
      THROW_InvalidArgument("required field 'real_fac' must be string");
    }
    return iter;
  }
  DynaMemTableFactory(const json& js, const SidePluginRepo& repo) {
    auto iter = Get_real_fac_iter(js, repo);
    orig_name = iter.value().get<std::string>();
  }
  void Update(const json&, const json& js, const SidePluginRepo& repo) {
    auto iter = Get_real_fac_iter(js, repo);
    auto& inner = iter.value();
    shared_ptr<MemTableRepFactory> real_fac; // intentional same name
    {
      std::lock_guard<std::mutex> lock(m_mtx);
      ROCKSDB_JSON_OPT_FACT_INNER(inner, real_fac);
      if (dynamic_cast<DynaMemTableFactory*>(real_fac.get())) {
        THROW_InvalidArgument("real_fac must not be DynaMemTableFactory");
      }
      this->real_fac = real_fac;
    }
  }
  std::string ToString(const json& d, const SidePluginRepo& repo) const {
    const bool html = JsonSmartBool(d, "html", true);
    json djs;
    ROCKSDB_JSON_SET_PROP(djs, orig_name);
    ROCKSDB_JSON_SET_FACX(djs, real_fac, memtable_factory);
    return JsonToString(djs, d);
  }
  void BackPatch(const SidePluginRepo& repo) {
    ROCKSDB_VERIFY(nullptr == real_fac);
    json js = { {"real_fac", orig_name } };
    Update({}, js, repo);
  }
  MemTableRep*
  CreateMemTableRep(const MemTableRep::KeyComparator& kc, Allocator* a,
                    const SliceTransform* st, Logger* logger) override {
    return real_fac->CreateMemTableRep(kc, a, st, logger);
  }
  MemTableRep*
  CreateMemTableRep(const MemTableRep::KeyComparator& kc, Allocator* a,
                    const SliceTransform* st, Logger* logger,
                    uint32_t cf_id) override {
    return real_fac->CreateMemTableRep(kc, a, st, logger, cf_id);
  }
  bool IsInsertConcurrentlySupported() const override {
    return real_fac->IsInsertConcurrentlySupported();
  }
  bool CanHandleDuplicatedKey() const override {
    return real_fac->CanHandleDuplicatedKey();
  }
  const char* Name() const override { return "Dyna"; }
};
// with dispatch, we can check conf at first place
void DynaMemTableBackPatch(MemTableRepFactory* f, const SidePluginRepo& repo) {
  auto dispatcher = dynamic_cast<DynaMemTableFactory*>(f);
  assert(nullptr != dispatcher);
  dispatcher->BackPatch(repo);
}
ROCKSDB_REG_Plugin("Dyna", DynaMemTableFactory, MemTableRepFactory);
ROCKSDB_REG_EasyProxyManip("Dyna", DynaMemTableFactory, MemTableRepFactory);

//////////////////////////////////////////////////////////////////////////////

static shared_ptr<TablePropertiesCollectorFactory>
JS_NewCompactOnDeletionCollectorFactory(const json& js, const SidePluginRepo&) {
  size_t sliding_window_size = 0;
  size_t deletion_trigger = 0;
  double deletion_ratio = 0;
  ROCKSDB_JSON_REQ_PROP(js, sliding_window_size);
  ROCKSDB_JSON_REQ_PROP(js, deletion_trigger);
  ROCKSDB_JSON_OPT_PROP(js, deletion_ratio);  // this is optional
  return NewCompactOnDeletionCollectorFactory(sliding_window_size,
                                              deletion_trigger, deletion_ratio);
}
ROCKSDB_FACTORY_REG("CompactOnDeletionCollector",
               JS_NewCompactOnDeletionCollectorFactory);

//////////////////////////////////////////////////////////////////////////////

static shared_ptr<RateLimiter>
JS_NewGenericRateLimiter(const json& js, const SidePluginRepo& repo) {
  int64_t rate_bytes_per_sec = 0;
  int64_t refill_period_us = 100 * 1000;
  int32_t fairness = 10;
  RateLimiter::Mode mode = RateLimiter::Mode::kWritesOnly;
  bool auto_tuned = false;
  ROCKSDB_JSON_REQ_SIZE(js, rate_bytes_per_sec); // required
  ROCKSDB_JSON_OPT_PROP(js, refill_period_us);
  ROCKSDB_JSON_OPT_PROP(js, fairness);
  ROCKSDB_JSON_OPT_ENUM(js, mode);
  ROCKSDB_JSON_OPT_PROP(js, auto_tuned);
  if (rate_bytes_per_sec <= 0) {
    THROW_InvalidArgument("rate_bytes_per_sec must > 0");
  }
  if (refill_period_us <= 0) {
    THROW_InvalidArgument("refill_period_us must > 0");
  }
  if (fairness <= 0) {
    THROW_InvalidArgument("fairness must > 0");
  }
  Env* env = Env::Default();
  if (auto iter = js.find("env"); js.end() != iter) {
    const auto& env_js = iter.value();
    env = PluginFactory<Env*>::GetPlugin("env", ROCKSDB_FUNC, env_js, repo);
    if (!env)
      THROW_InvalidArgument("param env is specified but got null");
  }
  return std::make_shared<GenericRateLimiter>(
      rate_bytes_per_sec, refill_period_us, fairness,
      mode,
#if ROCKSDB_VERSION >= 60203
      env->GetSystemClock(),
#else
      env,
#endif
      auto_tuned);
}
ROCKSDB_FACTORY_REG("GenericRateLimiter", JS_NewGenericRateLimiter);

//////////////////////////////////////////////////////////////////////////////

static std::shared_ptr<WriteBufferManager>
JS_NewWriteBufferManager(const json& js, const SidePluginRepo& repo) {
  size_t buffer_size = 0;
  shared_ptr<Cache> cache;
  ROCKSDB_JSON_REQ_SIZE(js, buffer_size);
  ROCKSDB_JSON_OPT_FACT(js, cache);
  return std::make_shared<WriteBufferManager>(buffer_size, cache);
}
struct WriteBufferManager_Manip : PluginManipFunc<WriteBufferManager> {
  void Update(WriteBufferManager* wbm,
              const json&, const json& js, const SidePluginRepo&) const final {
    size_t buffer_size = 0;
    ROCKSDB_JSON_REQ_SIZE(js, buffer_size);
    if (buffer_size) {
      wbm->SetBufferSize(buffer_size);
    }
  }
  std::string ToString(const WriteBufferManager& wbm, const json& dump_options,
                       const SidePluginRepo& repo) const final {
    bool html = JsonSmartBool(dump_options, "html", true);
    json js;
    size_t buffer_size = wbm.buffer_size();
    const shared_ptr<Cache>& cache = wbm.GetCache();
    ROCKSDB_JSON_SET_SIZE(js, buffer_size);
    ROCKSDB_JSON_SET_FACT(js, cache);
    return JsonToString(js, dump_options);
  }
};
ROCKSDB_FACTORY_REG("WriteBufferManager", JS_NewWriteBufferManager);
ROCKSDB_REG_PluginManip("WriteBufferManager", WriteBufferManager_Manip);

ROCKSDB_FACTORY_REG("Default", JS_NewWriteBufferManager);
ROCKSDB_REG_PluginManip("Default", WriteBufferManager_Manip);

/////////////////////////////////////////////////////////////////////////////

json JS_CompactionParamsEncodePtr(const CompactionParams* x) {
  // pass as object pointer(convert to uintptr_t)
  // because json does not allow non-utf8 binary data in string,
  // using encode/decode is tedious, so we keep it simple stupid.
  return json{{"ptr", uintptr_t(x)}};
}
const CompactionParams* JS_CompactionParamsDecodePtr(const json& js) {
  // "ptr" is passed by ptr's integer value
  uintptr_t ptr = 0;
  ROCKSDB_JSON_REQ_PROP(js, ptr);
  return reinterpret_cast<const CompactionParams*>(ptr);
}

} // ROCKSDB_NAMESPACE
