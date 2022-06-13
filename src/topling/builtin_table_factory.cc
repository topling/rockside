//
// Created by leipeng on 2020/7/1.
//
#include "logging/logging.h"
#include "table/block_based/block_based_table_factory.h"
#include "table/plain/plain_table_factory.h"
#include "table/table_builder.h"
#include "db/compaction/compaction.h"
#include "json.h"
#include "side_plugin_factory.h"
#include "builtin_table_factory.h"
#include "internal_dispather_table.h"

// when we can not get a rocksdb Logger object, use this simple log
#define PrintLog(level, fmt, ...) \
  do { if (SidePluginRepo::DebugLevel() >= level) \
    fprintf(stderr, "%s: " fmt "\n", StrDateTimeNow(), ## __VA_ARGS__); \
  } while (0)
#define TRAC(...) PrintLog(4, "TRAC: " __VA_ARGS__)
#define DEBG(...) PrintLog(3, "DEBG: " __VA_ARGS__)
#define INFO(...) PrintLog(2, "INFO: " __VA_ARGS__)
#define WARN(...) PrintLog(1, "WARN: " __VA_ARGS__)

namespace ROCKSDB_NAMESPACE {

const char* StrDateTimeNow() {
  static thread_local char buf[64];
  time_t rawtime;
  time(&rawtime);
  struct tm* timeinfo = localtime(&rawtime);
  strftime(buf, sizeof(buf), "%F %T",timeinfo);
  return buf;
}

static std::shared_ptr<const FilterPolicy>
NewBloomFilterPolicyJson(const json& js, const SidePluginRepo&) {
  double bits_per_key = 10;
  bool use_block_based_builder = false;
  ROCKSDB_JSON_OPT_PROP(js, bits_per_key);
  ROCKSDB_JSON_OPT_PROP(js, use_block_based_builder);
  return std::shared_ptr<const FilterPolicy>(
      NewBloomFilterPolicy(bits_per_key, use_block_based_builder));
}
ROCKSDB_FACTORY_REG("BloomFilter", NewBloomFilterPolicyJson);
ROCKSDB_FACTORY_REG("Bloom"      , NewBloomFilterPolicyJson);

static std::shared_ptr<const FilterPolicy>
NewRibbonFilterPolicyJson(const json& js, const SidePluginRepo&) {
  double bits_per_key = 10; // bloom_equivalent_bits_per_key
  int bloom_before_level = 0; // rocksdb-6.23: PR #8679
  ROCKSDB_JSON_REQ_PROP(js, bits_per_key);
  ROCKSDB_JSON_OPT_PROP(js, bloom_before_level);
  return std::shared_ptr<const FilterPolicy>(
      NewRibbonFilterPolicy(bits_per_key));
}
ROCKSDB_FACTORY_REG("RibbonFilter", NewRibbonFilterPolicyJson);
ROCKSDB_FACTORY_REG("Ribbon"      , NewRibbonFilterPolicyJson);

struct MetadataCacheOptions_Json : MetadataCacheOptions {
  explicit MetadataCacheOptions_Json(const json& js) {
    ROCKSDB_JSON_OPT_ENUM(js, top_level_index_pinning);
    ROCKSDB_JSON_OPT_ENUM(js, partition_pinning);
    ROCKSDB_JSON_OPT_ENUM(js, unpartitioned_pinning);
  }
  void SaveToJson(json& js) const {
    ROCKSDB_JSON_SET_ENUM(js, top_level_index_pinning);
    ROCKSDB_JSON_SET_ENUM(js, partition_pinning);
    ROCKSDB_JSON_SET_ENUM(js, unpartitioned_pinning);
  }
};
MetadataCacheOptions_Json NestForBase(const MetadataCacheOptions&);

struct BlockBasedTableOptions_Json : BlockBasedTableOptions {
  BlockBasedTableOptions_Json(const json& js, const SidePluginRepo& repo) {
    Update(js, repo);
  }
  void Update(const json& js, const SidePluginRepo& repo) {
    if (!IsCompactionWorker())
      ROCKSDB_JSON_OPT_FACT(js, flush_block_policy_factory);
    ROCKSDB_JSON_OPT_PROP(js, cache_index_and_filter_blocks);
    ROCKSDB_JSON_OPT_PROP(js, cache_index_and_filter_blocks_with_high_priority);
    ROCKSDB_JSON_OPT_PROP(js, pin_l0_filter_and_index_blocks_in_cache);
    ROCKSDB_JSON_OPT_PROP(js, pin_top_level_index_and_filter);
    ROCKSDB_JSON_OPT_NEST(js, metadata_cache_options);
    ROCKSDB_JSON_OPT_ENUM(js, index_type);
    ROCKSDB_JSON_OPT_ENUM(js, data_block_index_type);
    ROCKSDB_JSON_OPT_ENUM(js, index_shortening);
    ROCKSDB_JSON_OPT_ENUM(js, data_block_index_type);
    ROCKSDB_JSON_OPT_PROP(js, data_block_hash_table_util_ratio);
#if ROCKSDB_MAJOR < 7
    ROCKSDB_JSON_OPT_PROP(js, hash_index_allow_collision);
#endif
    ROCKSDB_JSON_OPT_ENUM(js, checksum);
    ROCKSDB_JSON_OPT_PROP(js, no_block_cache);
    ROCKSDB_JSON_OPT_SIZE(js, block_size);
    ROCKSDB_JSON_OPT_PROP(js, block_size_deviation);
    ROCKSDB_JSON_OPT_PROP(js, block_restart_interval);
    ROCKSDB_JSON_OPT_PROP(js, index_block_restart_interval);
    ROCKSDB_JSON_OPT_SIZE(js, metadata_block_size);
#if (ROCKSDB_MAJOR * 10000 + ROCKSDB_MINOR * 10 + ROCKSDB_PATCH) >= 60280
  #if (ROCKSDB_MAJOR * 10000 + ROCKSDB_MINOR * 10 + ROCKSDB_PATCH) >= 70030
    ROCKSDB_JSON_OPT_SIZE(js, initial_auto_readahead_size);
  #else
    ROCKSDB_JSON_OPT_PROP(js, reserve_table_builder_memory);
  #endif
#endif
    ROCKSDB_JSON_OPT_PROP(js, partition_filters);
    ROCKSDB_JSON_OPT_PROP(js, optimize_filters_for_memory);
    ROCKSDB_JSON_OPT_PROP(js, use_delta_encoding);
    ROCKSDB_JSON_OPT_PROP(js, use_raw_size_as_estimated_file_size);
    ROCKSDB_JSON_OPT_PROP(js, read_amp_bytes_per_bit);
    ROCKSDB_JSON_OPT_PROP(js, whole_key_filtering);
    ROCKSDB_JSON_OPT_PROP(js, verify_compression);
    ROCKSDB_JSON_OPT_PROP(js, format_version);
    ROCKSDB_JSON_OPT_PROP(js, enable_index_compression);
    ROCKSDB_JSON_OPT_PROP(js, block_align);
    if (!IsCompactionWorker()) {
      ROCKSDB_JSON_OPT_FACT(js, block_cache);
      ROCKSDB_JSON_OPT_FACT(js, block_cache_compressed);
      ROCKSDB_JSON_OPT_FACT(js, persistent_cache);
      ROCKSDB_JSON_OPT_FACT(js, filter_policy);
    }
    ROCKSDB_JSON_OPT_SIZE(js, max_auto_readahead_size);
    ROCKSDB_JSON_OPT_ENUM(js, prepopulate_block_cache);
    ROCKSDB_JSON_OPT_PROP(js, enable_get_random_keys);
  }

  json ToJsonObj(const json& dump_options, const SidePluginRepo& repo) const {
    bool html = JsonSmartBool(dump_options, "html", true);
    json js;
    if (!IsCompactionWorker())
      ROCKSDB_JSON_SET_FACT(js, flush_block_policy_factory);
    ROCKSDB_JSON_SET_PROP(js, cache_index_and_filter_blocks);
    ROCKSDB_JSON_SET_PROP(js, cache_index_and_filter_blocks_with_high_priority);
    ROCKSDB_JSON_SET_PROP(js, pin_l0_filter_and_index_blocks_in_cache);
    ROCKSDB_JSON_SET_PROP(js, pin_top_level_index_and_filter);
    ROCKSDB_JSON_SET_NEST(js, metadata_cache_options);
    ROCKSDB_JSON_SET_ENUM(js, index_type);
    ROCKSDB_JSON_SET_ENUM(js, data_block_index_type);
    ROCKSDB_JSON_SET_ENUM(js, index_shortening);
    ROCKSDB_JSON_SET_ENUM(js, data_block_index_type);
    ROCKSDB_JSON_SET_PROP(js, data_block_hash_table_util_ratio);
#if ROCKSDB_MAJOR < 7
    ROCKSDB_JSON_SET_PROP(js, hash_index_allow_collision);
#endif
    ROCKSDB_JSON_SET_ENUM(js, checksum);
    ROCKSDB_JSON_SET_PROP(js, no_block_cache);
    ROCKSDB_JSON_SET_SIZE(js, block_size);
    ROCKSDB_JSON_SET_PROP(js, block_size_deviation);
    ROCKSDB_JSON_SET_PROP(js, block_restart_interval);
    ROCKSDB_JSON_SET_PROP(js, index_block_restart_interval);
    ROCKSDB_JSON_SET_SIZE(js, metadata_block_size);
#if (ROCKSDB_MAJOR * 10000 + ROCKSDB_MINOR * 10 + ROCKSDB_PATCH) >= 60280
  #if (ROCKSDB_MAJOR * 10000 + ROCKSDB_MINOR * 10 + ROCKSDB_PATCH) >= 70030
    ROCKSDB_JSON_SET_SIZE(js, initial_auto_readahead_size);
  #else
    ROCKSDB_JSON_SET_PROP(js, reserve_table_builder_memory);
  #endif
#endif
    ROCKSDB_JSON_SET_PROP(js, partition_filters);
    ROCKSDB_JSON_SET_PROP(js, optimize_filters_for_memory);
    ROCKSDB_JSON_SET_PROP(js, use_delta_encoding);
    ROCKSDB_JSON_SET_PROP(js, use_raw_size_as_estimated_file_size);
    ROCKSDB_JSON_SET_PROP(js, read_amp_bytes_per_bit);
    ROCKSDB_JSON_SET_PROP(js, whole_key_filtering);
    ROCKSDB_JSON_SET_PROP(js, verify_compression);
    ROCKSDB_JSON_SET_PROP(js, format_version);
    ROCKSDB_JSON_SET_PROP(js, enable_index_compression);
    ROCKSDB_JSON_SET_PROP(js, block_align);
    if (!IsCompactionWorker()) {
      ROCKSDB_JSON_SET_FACX(js, block_cache, cache);
      ROCKSDB_JSON_SET_FACX(js, block_cache_compressed, cache);
      ROCKSDB_JSON_SET_FACT(js, persistent_cache);
      ROCKSDB_JSON_SET_FACT(js, filter_policy);
    }
    ROCKSDB_JSON_SET_SIZE(js, max_auto_readahead_size);
    ROCKSDB_JSON_SET_ENUM(js, prepopulate_block_cache);
    ROCKSDB_JSON_SET_PROP(js, enable_get_random_keys);
    return js;
  }
  std::string ToJsonStr(const json& dump_options,
                        const SidePluginRepo& repo) const {
    auto js = ToJsonObj(dump_options, repo);
    return JsonToString(js, dump_options);
  }
};

static std::shared_ptr<TableFactory>
NewBlockBasedTableFactoryFromJson(const json& js, const SidePluginRepo& repo) {
  BlockBasedTableOptions_Json _table_options(js, repo);
  return std::make_shared<BlockBasedTableFactory>(_table_options);
}
ROCKSDB_FACTORY_REG("BlockBasedTable", NewBlockBasedTableFactoryFromJson);
ROCKSDB_FACTORY_REG("BlockBased", NewBlockBasedTableFactoryFromJson);

struct BlockBasedTableFactory_Manip : PluginManipFunc<TableFactory> {
  void Update(TableFactory* p, const json&, const json& js,
              const SidePluginRepo& repo) const final {
    if (auto t = dynamic_cast<BlockBasedTableFactory*>(p)) {
      auto o = static_cast<const BlockBasedTableOptions_Json&>(t->table_options());
      auto mo = const_cast<BlockBasedTableOptions_Json&>(o);
      mo.Update(js, repo);
      return;
    }
    std::string name = p->Name();
    THROW_InvalidArgument("Is not BlockBasedTable, but is: " + name);
  }
  std::string ToString(const TableFactory& fac, const json& dump_options,
                       const SidePluginRepo& repo) const final {
    if (auto t = dynamic_cast<const BlockBasedTableFactory*>(&fac)) {
      auto o = static_cast<const BlockBasedTableOptions_Json&>(t->table_options());
      return o.ToJsonStr(dump_options, repo);
    }
    std::string name = fac.Name();
    THROW_InvalidArgument("Is not BlockBasedTable, but is: " + name);
  }
};

static const PluginManipFunc<TableFactory>*
JS_BlockBasedTableFactoryManip(const json&, const SidePluginRepo&) {
  static const BlockBasedTableFactory_Manip manip;
  return &manip;
}
ROCKSDB_FACTORY_REG("BlockBased", JS_BlockBasedTableFactoryManip);
ROCKSDB_FACTORY_REG("BlockBasedTable", JS_BlockBasedTableFactoryManip);

////////////////////////////////////////////////////////////////////////////
struct PlainTableOptions_Json : PlainTableOptions {
  PlainTableOptions_Json(const json& js) {
    ROCKSDB_JSON_OPT_PROP(js, user_key_len);
    ROCKSDB_JSON_OPT_PROP(js, bloom_bits_per_key);
    ROCKSDB_JSON_OPT_PROP(js, hash_table_ratio);
    ROCKSDB_JSON_OPT_PROP(js, index_sparseness);
    ROCKSDB_JSON_OPT_ENUM(js, encoding_type);
    ROCKSDB_JSON_OPT_PROP(js, full_scan_mode);
    ROCKSDB_JSON_OPT_PROP(js, store_index_in_file);
  }
};
static std::shared_ptr<TableFactory>
NewPlainTableFactoryFromJson(const json& js, const SidePluginRepo&) {
  PlainTableOptions_Json options(js);
  return std::make_shared<PlainTableFactory>(options);
}
ROCKSDB_FACTORY_REG("PlainTable", NewPlainTableFactoryFromJson);

////////////////////////////////////////////////////////////////////////////
struct CuckooTableOptions_Json : CuckooTableOptions {
  CuckooTableOptions_Json(const json& js) {
    ROCKSDB_JSON_OPT_PROP(js, hash_table_ratio);
    ROCKSDB_JSON_OPT_PROP(js, max_search_depth);
    ROCKSDB_JSON_OPT_PROP(js, hash_table_ratio);
    ROCKSDB_JSON_OPT_PROP(js, cuckoo_block_size);
    ROCKSDB_JSON_OPT_PROP(js, identity_as_first_hash);
    ROCKSDB_JSON_OPT_PROP(js, use_module_hash);
  }
};
static std::shared_ptr<TableFactory>
NewCuckooTableFactoryJson(const json& js, const SidePluginRepo&) {
  CuckooTableOptions_Json options(js);
  return std::shared_ptr<TableFactory>(NewCuckooTableFactory(options));
}
ROCKSDB_FACTORY_REG("CuckooTable", NewCuckooTableFactoryJson);

////////////////////////////////////////////////////////////////////////////

extern const uint64_t kPlainTableMagicNumber;
extern const uint64_t kLegacyPlainTableMagicNumber;
extern const uint64_t kBlockBasedTableMagicNumber;
extern const uint64_t kLegacyBlockBasedTableMagicNumber;
extern const uint64_t kCuckooTableMagicNumber;

// plugin TableFactory can using this function to register
// its TableMagicNumber
std::map<uint64_t, std::string>&
GetDispatcherTableMagicNumberMap() {
  static std::map<uint64_t, std::string> map {
      {kPlainTableMagicNumber, "PlainTable"},
      {kLegacyPlainTableMagicNumber, "PlainTable"},
      {kBlockBasedTableMagicNumber, "BlockBasedTable"},
      {kLegacyBlockBasedTableMagicNumber, "BlockBasedTable"},
      {kCuckooTableMagicNumber, "CuckooTable"},
  };
  return map;
}
RegTableFactoryMagicNumber::
RegTableFactoryMagicNumber(uint64_t magic, const char* name) {
  auto ib = GetDispatcherTableMagicNumberMap().emplace(magic, name);
  using ll = long long;
  ROCKSDB_VERIFY_F(ib.second, "Dup MagicNum: %016llX -> %s", ll(magic), name);
}

////////////////////////////////////////////////////////////////////////////
static std::shared_ptr<SstPartitionerFactory>
NewFixedPrefixPartitionerFactoryJson(const json& js, const SidePluginRepo&) {
  size_t prefix_len = 0;
  ROCKSDB_JSON_REQ_PROP(js, prefix_len);
  return NewSstPartitionerFixedPrefixFactory(prefix_len);
}
ROCKSDB_FACTORY_REG("SstPartitionerFixedPrefixFactory",
                    NewFixedPrefixPartitionerFactoryJson);
ROCKSDB_FACTORY_REG("SstPartitionerFixedPrefix",
                    NewFixedPrefixPartitionerFactoryJson);
ROCKSDB_FACTORY_REG("FixedPrefix", NewFixedPrefixPartitionerFactoryJson);

////////////////////////////////////////////////////////////////////////////

using namespace std::chrono;
class DispatcherTableFactory;

// for hook TableBuilder::Add(key, value) to perform statistics
class DispatcherTableBuilder : public TableBuilder {
 public:
  TableBuilder* tb = nullptr;
  const DispatcherTableFactory* dispatcher = nullptr;
  using Stat = DispatcherTableFactory::Stat;
  Stat st;
  Stat st_sum;
  int  level;
  DispatcherTableBuilder(TableBuilder* tb1,
                        const DispatcherTableFactory* dtf1,
                        int level1);
  ~DispatcherTableBuilder();
  void UpdateStat();
  void Add(const Slice& key, const Slice& value) final;
  Status status() const final { return tb->status(); }
  IOStatus io_status() const final { return tb->io_status(); }
  Status Finish() final { return tb->Finish(); }
  void Abandon() final { tb->Abandon(); }
  uint64_t NumEntries() const final { return tb->NumEntries(); }
  bool IsEmpty() const { return tb->IsEmpty(); }
  uint64_t FileSize() const final { return tb->FileSize(); }
  uint64_t EstimatedFileSize() const { return tb->EstimatedFileSize(); }
  bool NeedCompact() const { return tb->NeedCompact(); }
  TableProperties GetTableProperties() const final { return tb->GetTableProperties(); }
  std::string GetFileChecksum() const final { return tb->GetFileChecksum(); }
  const char* GetFileChecksumFuncName() const final { return tb->GetFileChecksumFuncName(); }
};

DispatcherTableFactory::~DispatcherTableFactory() {}

DispatcherTableFactory::
DispatcherTableFactory(const json& js, const SidePluginRepo& repo) {
  m_json_obj = js; // backup
  m_json_str = js.dump();
  m_is_back_patched = false;
  m_is_delete_range_supported = false;
  ignoreInputCompressionMatchesOutput = false;
  ROCKSDB_JSON_OPT_PROP(js, ignoreInputCompressionMatchesOutput);
}

const char* DispatcherTableFactory::Name() const {
  return "DispatcherTable";
}

Status DispatcherTableFactory::NewTableReader(
    const ReadOptions& ro,
    const TableReaderOptions& table_reader_options,
    std::unique_ptr<RandomAccessFileReader>&& file, uint64_t file_size,
    std::unique_ptr<TableReader>* table,
    bool prefetch_index_and_filter_in_cache) const {
  if (!m_is_back_patched) {
    return Status::InvalidArgument(
        ROCKSDB_FUNC, "BackPatch() was not called");
  }
  auto info_log = table_reader_options.ioptions.info_log.get();
  Footer footer;
  auto s = ReadFooterFromFile(IOOptions(),
                              file.get(), nullptr /* prefetch_buffer */,
                              file_size, &footer);
  if (!s.ok()) {
    return s;
  }
  const char* func = "Dispatch::NewTableReader"; // short func name
  auto magic = (unsigned long long)footer.table_magic_number();
  auto fp_iter = m_magic_to_factory.find(magic);
  if (m_magic_to_factory.end() != fp_iter) {
    const std::shared_ptr<TableFactory>& factory = fp_iter->second.factory;
    const std::string&                   varname = fp_iter->second.varname;
    ROCKS_LOG_DEBUG(info_log, "%s: found factory: %016llX : %s: %s\n",
         func, magic, factory->Name(), varname.c_str());
    fp_iter->second.open_cnt++;
    fp_iter->second.sum_open_size += file_size;
    return factory->NewTableReader(ro, table_reader_options,
                                   std::move(file), file_size, table,
                                   prefetch_index_and_filter_in_cache);
  }
  auto& map = GetDispatcherTableMagicNumberMap();
  auto iter = map.find(magic);
  if (map.end() != iter) {
    const std::string& facname = iter->second;
    if (PluginFactorySP<TableFactory>::HasPlugin(facname)) {
      try {
        ROCKS_LOG_WARN(info_log,
            "%s: not found factory: %016llX : %s, onfly create it.\n",
            func, magic, facname.c_str());
        json null_js;
        SidePluginRepo empty_repo;
        auto factory = PluginFactorySP<TableFactory>::
                AcquirePlugin(facname, null_js, empty_repo);
        return factory->NewTableReader(ro, table_reader_options,
                                       std::move(file), file_size, table,
                                       prefetch_index_and_filter_in_cache);
      }
      catch (const std::exception& ex) {
        return Status::Corruption(ROCKSDB_FUNC,
          "onfly create factory=\"" + facname + "\" failed: " + ex.what());
      }
      catch (const Status& es) {
        return Status::Corruption(ROCKSDB_FUNC,
          "onfly create factory=\"" + facname + "\" failed: " + es.ToString());
      }
    } else {
      std::string msg;
      msg += "MagicNumber = ";
      msg += Slice((char*)&iter->first, 8).ToString(true);
      msg += " -> Factory class = " + facname;
      msg += " is defined in TableMagicNumberMap but not registered as a plugin.";
      return Status::Corruption(ROCKSDB_FUNC, msg);
    }
  }
  return Status::NotSupported("Unidentified table format");
}

TableBuilder* DispatcherTableFactory::NewTableBuilder(
    const TableBuilderOptions& tbo,
    WritableFileWriter* file)
const {
  auto info_log = tbo.ioptions.info_log.get();
  ROCKSDB_VERIFY_F(m_is_back_patched, "BackPatch() was not called");
  int level = std::min(tbo.level_at_creation, int(m_level_writers.size()-1));
  TableBuilder* builder;
  if (level >= 0) {
    ROCKS_LOG_DEBUG(info_log,
        "Dispatch::NewTableBuilder: level = %d, use level factory = %s\n",
        level, m_level_writers[level]->Name());
    builder = m_level_writers[level]->NewTableBuilder(tbo, file);
    if (!builder) {
      ROCKS_LOG_WARN(info_log,
        "Dispatch::NewTableBuilder: level = %d, use level factory = %s,"
        " returns null, try default: %s\n",
        level, m_level_writers[level]->Name(), m_default_writer->Name());
      builder = m_default_writer->NewTableBuilder(tbo, file);
      m_writer_files[0]++;
    } else {
      m_writer_files[level+1]++;
    }
  }
  else {
    ROCKS_LOG_DEBUG(info_log,
        "Dispatch::NewTableBuilder: level = %d, use default factory = %s\n",
        level, m_default_writer->Name());
    builder = m_default_writer->NewTableBuilder(tbo, file);
    m_writer_files[0]++;
  }
  return new DispatcherTableBuilder(builder, this, level);
}

// Sanitizes the specified DB Options.
Status DispatcherTableFactory::ValidateOptions(const DBOptions&, const ColumnFamilyOptions&)
const {
  return Status::OK();
}

bool DispatcherTableFactory::IsDeleteRangeSupported() const {
  return m_is_delete_range_supported;
}

static std::string str_input_levels(const Compaction* c) {
  ROCKSDB_VERIFY(!c->inputs()->empty());
  std::string s;
  char buf[32];
  for (auto& each_input : *c->inputs()) {
    s.append(buf, sprintf(buf, "%d,", each_input.level));
  }
  s.pop_back();
  return s;
}

bool DispatcherTableFactory::InputCompressionMatchesOutput(const Compaction* c) const {
  if (ignoreInputCompressionMatchesOutput) {
    return true;
  }
  auto get_fac = [&](int level) {
    level = std::min(level, int(m_level_writers.size()) - 1);
    if (level < 0) {
      return m_default_writer.get();
    } else {
      return m_level_writers[level].get();
    }
  };
  auto debug_print = [c](const char* strbool) {
    ROCKS_LOG_DEBUG(c->immutable_options()->info_log.get(),
        "DispatcherTableFactory::InputCompressionMatchesOutput: {%s}->%d : %s",
        str_input_levels(c).c_str(), c->output_level(), strbool);
  };
  const TableFactory* output_factory = get_fac(c->output_level());
  for (auto& each_input : *c->inputs()) {
    const TableFactory* input_factory = get_fac(each_input.level);
    if (input_factory != output_factory) {
      debug_print("false");
      return false;
    }
  }
  debug_print("true");
  return true;
}

void DispatcherTableBackPatch(TableFactory* f, const SidePluginRepo& repo) {
  auto dispatcher = dynamic_cast<DispatcherTableFactory*>(f);
  assert(nullptr != dispatcher);
  dispatcher->BackPatch(repo);
}

extern bool IsCompactionWorker();
void DispatcherTableFactory::BackPatch(const SidePluginRepo& repo) {
  ROCKSDB_VERIFY_F(!m_is_back_patched, "BackPatch() was already called");
  ROCKSDB_VERIFY(m_all.get() == nullptr);
  m_all = repo.m_impl->table_factory.name2p;
  if (!m_json_obj.is_object()) {
    THROW_InvalidArgument("DispatcherTableFactory options must be object");
  }
  auto iter = m_json_obj.find("default");
  if (m_json_obj.end() != iter) {
    auto& subjs = iter.value();
    m_default_writer = PluginFactorySP<TableFactory>::
      GetPlugin("default", ROCKSDB_FUNC, subjs, repo);
    if (!m_default_writer) {
      THROW_InvalidArgument("fail get defined default writer = " + subjs.dump());
    }
  } else {
    auto iter2 = m_all->find("default");
    if (m_all->end() != iter2) {
      m_default_writer = iter2->second;
    } else {
     THROW_InvalidArgument("fail get global default Factory");
    }
  }
  iter = m_json_obj.find("level_writers");
  if (m_json_obj.end() != iter) {
    auto& level_writers_js = iter.value();
    if (!level_writers_js.is_array()) {
      THROW_InvalidArgument("level_writers must be a json array");
    }
    if (level_writers_js.empty()) {
      THROW_InvalidArgument("level_writers must be a non-empty json array");
    }
    m_level_writers.reserve(level_writers_js.size());
    for (auto& item : level_writers_js.items()) {
      char varname[32];
      sprintf(varname, "level_writers[%zd]", m_level_writers.size());
      auto& options = item.value();
      auto p = PluginFactorySP<TableFactory>::
        GetPlugin(varname, ROCKSDB_FUNC, options, repo);
      if (!p) {
        THROW_InvalidArgument(
            "ObtainPlugin returns null, options = " + options.dump());
      }
      m_level_writers.push_back(p);
    }
  }
  for (auto& stv : m_stats) {
    stv.resize(m_level_writers.size() + 1);
  }
  m_writer_files.resize(m_level_writers.size() + 1);
  std::map<std::string, std::vector<uint64_t> > name2magic;
  for (auto& kv : GetDispatcherTableMagicNumberMap()) {
    name2magic[kv.second].push_back(kv.first);
    TRAC("Add name2magic: %016llX : %s", (long long)kv.first, kv.second.c_str());
  }
  auto add_magic = [&](const std::shared_ptr<TableFactory>& factory,
                       const std::string& varname,
                       bool is_user_defined) {
    const char* facname = factory->Name();
    if (strcmp(facname, this->Name()) == 0) {
      if (is_user_defined) {
        THROW_InvalidArgument("Dispatch factory can not be defined as a reader");
      }
      return;
    }
    auto it = name2magic.find(facname);
    if (name2magic.end() == it) {
      THROW_InvalidArgument(
          std::string("not found magic of factory: ") + facname);
    }
    for (uint64_t magic : it->second) {
      ReaderFactory rf;
      rf.factory = factory;
      rf.varname = varname;
      rf.is_user_defined = is_user_defined;
      auto ib = m_magic_to_factory.emplace(magic, rf);
      if (!ib.second) { // emplace fail
        const char* varname1 = ib.first->second.varname.c_str(); // existed
        const char* type = ib.first->second.is_user_defined ? "user" : "auto";
        TRAC("Dispatch::BackPatch: dup factory: %016llX : %-20s : %s(%s) %s(auto)",
             (long long)magic, facname, varname1, type, varname.c_str());
      } else {
        TRAC("Dispatch::BackPatch: reg factory: %016llX : %-20s : %s",
             (long long)magic, facname, varname.c_str());
      }
    }
  };
  iter = m_json_obj.find("readers");
  if (m_json_obj.end() != iter) {
    auto& readers_js = iter.value();
    if (!readers_js.is_object()) {
      THROW_InvalidArgument("readers must be a json object");
    }
    for (auto& item : readers_js.items()) {
      auto& facname = item.key();
      if (!item.value().is_string()) {
        THROW_InvalidArgument(
            "readers[\"" + facname + "\"] must be a json string");
      }
      if (!PluginFactorySP<TableFactory>::HasPlugin(facname)) {
        THROW_InvalidArgument(
            "facname = \"" + facname + "\" is not a plugin");
      }
      const std::string& varname = item.value().get_ref<const std::string&>();
      // facname is the param 'varname' for GetPlugin
      auto p = PluginFactorySP<TableFactory>::GetPlugin(
          facname.c_str(), ROCKSDB_FUNC, item.value(), repo);
      if (!p) {
        THROW_InvalidArgument(
            "undefined varname = " + varname + "(factory = " + facname + ")");
      }
      if (!PluginFactorySP<TableFactory>::SamePlugin(p->Name(), facname)) {
        THROW_InvalidArgument(
            "facname = " + facname + " but factory->Name() = " + p->Name());
      }
      add_magic(p, varname, true);
    }
  }
  for (auto& kv : *m_all) {
    auto& varname = kv.first; // factory varname
    auto& factory = kv.second;
    add_magic(factory, varname, false);
  }
  for (auto& kv : *m_all) {
    auto& factory = kv.second;
    const json* spec = repo.GetCreationSpec(factory);
    m_factories_spec.emplace_back(factory.get(), spec);
  }
  std::sort(m_factories_spec.begin(), m_factories_spec.end());
  m_json_obj = json{}; // reset
  m_is_back_patched = true;
  m_is_delete_range_supported = true;
  for (auto& kv : *m_all) {
    auto& varname = kv.first; // factory varname
    auto& factory = kv.second;
    if (!factory->IsDeleteRangeSupported()) {
      m_is_delete_range_supported = false;
      INFO("Dispatch::BackPatch: '%s' IsDeleteRangeSupported = false", varname.c_str());
    }
  }
  DEBG("Dispatch::BackPatch: IsDeleteRangeSupported = %d", m_is_delete_range_supported);
}

std::string DispatcherTableFactory::GetPrintableOptions() const {
  return m_json_str;
}

json DispatcherTableFactory::ToJsonObj(const json& dump_options, const SidePluginRepo& repo) const {
  const bool html = JsonSmartBool(dump_options, "html", true);
  const bool nozero = JsonSmartBool(dump_options, "nozero");
  auto& p2name = repo.m_impl->table_factory.p2name;
  const static std::string labels[] = {
       "1s-ops",  "1s-key",  "1s-val",
       "5s-ops",  "5s-key",  "5s-val",
      "30s-ops", "30s-key", "30s-val",
       "5m-ops",  "5m-key",  "5m-val",
      "30m-ops", "30m-key", "30m-val",
  };
  json lwjs;
  size_t non_zero_num = 0;
  auto add_writer = [&](const std::shared_ptr<TableFactory>& tf, size_t level) {
    size_t file_num = m_writer_files[level];
    if (nozero && 0 == file_num) {
      return;
    }
    if (0 != file_num) {
      non_zero_num++;
    }
    json wjs;
    char buf[64];
#define ToStr(...) json(std::string(buf, snprintf(buf, sizeof(buf), __VA_ARGS__)))
    wjs["level"] = 0 == level ? json("default") : json(level-1);
    ROCKSDB_JSON_SET_FACT_INNER(wjs["factory"], tf, table_factory);
    const auto& st = m_stats[0][level];
    wjs["files"] = file_num;
    if (html) {
      wjs["entry_cnt"] = ToStr("%.3f M", st.st.entry_cnt/1e6);
      wjs["key_size"] = ToStr("%.3f G", st.st.key_size/1e9);
      wjs["val_size"] = ToStr("%.3f G", st.st.val_size/1e9);
      wjs["avg_file"] = ToStr("%.3f M", st.st.file_size/1e6/file_num);
    } else {
      wjs["entry_cnt"] = st.st.entry_cnt;
      wjs["key_size"] = st.st.key_size;
      wjs["val_size"] = st.st.val_size;
      wjs["avg_file"] = double(st.st.file_size) / file_num;
    }
    for (size_t j = 0; j < 5; ++j) {
      double cnt = st.st.entry_cnt - m_stats[j+1][level].st.entry_cnt;
      double key = st.st.key_size - m_stats[j+1][level].st.key_size;
      double val = st.st.val_size - m_stats[j+1][level].st.val_size;
      double us = duration_cast<microseconds>(st.time - m_stats[j+1][level].time).count();
      wjs[labels[3*j+0]] = !cnt ? json(0) : ToStr("%.3f K/s", cnt/us*1e3);
      wjs[labels[3*j+1]] = !key ? json(0) : ToStr("%.3f M/s", key/us);
      wjs[labels[3*j+2]] = !val ? json(0) : ToStr("%.3f M/s", val/us);
    }
    lwjs.push_back(std::move(wjs));
  };
  json js;
  ROCKSDB_JSON_SET_PROP(js["options"], ignoreInputCompressionMatchesOutput);
  for (size_t i = 0, n = m_level_writers.size(); i < n; ++i) {
    auto& tf = m_level_writers[i];
    auto iter = p2name.find(tf.get());
    if (p2name.end() == iter) {
      THROW_Corruption("missing TableFactory of m_level_writer");
    }
    add_writer(tf, i+1);
  }
  if (html && !m_level_writers.empty()) {
    auto& cols = lwjs[0]["<htmltab:col>"];
    cols = json::array({
        "level", "factory", "files", "avg_file",
        "entry_cnt", "key_size", "val_size",
    });
    for (auto& lab : labels) {
      cols.push_back(lab);
    }
  }
  add_writer(m_default_writer, 0);
  if (lwjs.empty()) {
    lwjs = "Did Not Created Any TableBuilder, try nozero=0";
  }
  if (html && non_zero_num) {
    char buf[128];
    auto len = sprintf(buf, "writers<br/>"
      R"(<a href='javascript:SetParam("nozero","%d")'>nozero=%d</a>)",
      !nozero, !nozero);
    js[std::string(buf, len)] = std::move(lwjs);
  }
  else {
    js["writers"] = std::move(lwjs);
  }

  json readers_js;
  for (auto& kv : m_magic_to_factory) {
    size_t len = kv.second.sum_open_size;
    size_t cnt = kv.second.open_cnt;
    if (nozero && 0 == cnt) {
      continue;
    }
    json one_js;
    char buf[64];
    one_js["class"] = kv.second.factory->Name();
    one_js["magic_num"] = ToStr("%llX", (long long)kv.first);
    ROCKSDB_JSON_SET_FACT_INNER(one_js["factory"], kv.second.factory, table_factory);
    one_js["open_cnt"] = cnt;
    one_js["sum_open_size"] = ToStr("%.3f G", len/1e9);
    one_js["avg_open_size"] = ToStr("%.3f M", len/1e6/cnt);
    readers_js.push_back(std::move(one_js));
  }
  if (readers_js.empty()) {
    readers_js = "Did Not Created Any TableReader, try nozero=0";
  }
  else {
    readers_js[0]["<htmltab:col>"] = json::array({
      "class", "magic_num", "factory", "open_cnt", "sum_open_size",
      "avg_open_size"
    });
  }
  js["readers"] = std::move(readers_js);
  return js;
}
std::string DispatcherTableFactory::ToJsonStr(const json& dump_options,
                                              const SidePluginRepo& repo) const {
  if (JsonSmartBool(dump_options, "metric")) {
    return MetricStr(dump_options, repo);
  }
  auto js = ToJsonObj(dump_options, repo);
  try {
    return JsonToString(js, dump_options);
  }
  catch (const std::exception& ex) {
    THROW_InvalidArgument(std::string(ex.what()) + ", json:\n" + js.dump());
  }
}

template<class T>
static std::ostringstream& operator|(std::ostringstream& oss, const T& x) {
  oss << x;
  return oss;
}

std::string DispatcherTableFactory::
MetricStr(const json& dump_options, const SidePluginRepo& repo) const {
  std::ostringstream oss;
  for (size_t level = 0; level < m_level_writers.size(); ++level) {
    auto& st = m_stats[0][level+1];
    oss|"level:"|level|":file_cnt " |m_writer_files[level+1]|"\n";
    oss|"level:"|level|":file_size "|st.st.file_size|"\n";
    oss|"level:"|level|":entry_cnt "|st.st.entry_cnt|"\n";
    oss|"level:"|level|":key_size " |st.st.key_size |"\n";
    oss|"level:"|level|":val_size " |st.st.val_size |"\n";
  }
  {
    auto& st = m_stats[0][0];
    const char* level = "unknown";
    oss|"level:"|level|":file_cnt " |m_writer_files[0]|"\n";
    oss|"level:"|level|":file_size "|st.st.file_size|"\n";
    oss|"level:"|level|":entry_cnt "|st.st.entry_cnt|"\n";
    oss|"level:"|level|":key_size " |st.st.key_size |"\n";
    oss|"level:"|level|":val_size " |st.st.val_size |"\n";
  }
  for (auto& kv : m_magic_to_factory) {
    auto& v = kv.second;
    oss|"reader:"|v.varname|":open_cnt " |v.open_cnt|"\n";
    oss|"reader:"|v.varname|":sum_open_size " |v.sum_open_size|"\n";
  }
  return oss.str();
}

void DispatcherTableFactory::UpdateOptions(const json& js, const SidePluginRepo& repo) {
  ROCKSDB_JSON_OPT_PROP(js, ignoreInputCompressionMatchesOutput);
  auto iter = js.find("level_writers");
  if (js.end() != iter) {
    auto& level_writers_js = iter.value();
    if (!level_writers_js.is_array()) {
      THROW_InvalidArgument("level_writers must be a json array");
    }
    if (level_writers_js.empty()) {
      THROW_InvalidArgument("level_writers must be a non-empty json array");
    }
    size_t num = std::min(level_writers_js.size(), m_level_writers.size());
    for (size_t i = 0; i < num; ++i) {
      char varname[32];
      sprintf(varname, "level_writers[%zd]", i);
      auto& options = level_writers_js[i];
      if (options.is_null()) {
        continue; // dont change level_writers[i]
      }
      if (options.is_string()) {
         auto& str = options.get_ref<const std::string&>();
         if (str.empty() || "null" == str || "nullptr" == str) {
           continue; // dont change level_writers[i]
         }
      }
      auto p = PluginFactorySP<TableFactory>::
        GetPlugin(varname, ROCKSDB_FUNC, options, repo);
      if (!p) {
        THROW_InvalidArgument(
            "GetPlugin returns null, options = " + options.dump());
      }
      m_level_writers[i] = p;
    }
  }
  iter = js.find("default");
  if (js.end() != iter) {
    auto& subjs = iter.value();
    auto default_writer = PluginFactorySP<TableFactory>::
      GetPlugin("default", ROCKSDB_FUNC, subjs, repo);
    if (!default_writer) {
      THROW_InvalidArgument("fail get defined default writer = " + subjs.dump());
    }
    m_default_writer = default_writer;
  }
}

static const seconds g_durations[6] = {
    seconds(0),
    seconds(1),
    seconds(5),
    seconds(30),
    seconds(300), // 5 minutes
    seconds(1800), // 30 minutes
};

DispatcherTableBuilder::DispatcherTableBuilder(TableBuilder* tb1,
                      const DispatcherTableFactory* dtf1,
                      int level1) {
  tb = tb1;
  dispatcher = dtf1;
  if (size_t(level1) < dtf1->m_level_writers.size())
    level = level1 + 1;
  else
    level = 0;
}

template<class T>
inline
std::atomic<T>& as_atomic(T& x) {
    return reinterpret_cast<std::atomic<T>&>(x);
}

DispatcherTableBuilder::~DispatcherTableBuilder() {
  UpdateStat();
  as_atomic(dispatcher->m_stats[0][level].st.file_size)
    .fetch_add(tb->FileSize(), std::memory_order_relaxed);
  delete tb;
}

void DispatcherTableBuilder::Add(const Slice& key, const Slice& value) {
  st.entry_cnt++;
  st.key_size += key.size();
  st.val_size += value.size();
  if (UNLIKELY(st.entry_cnt % 1024 == 0)) {
    UpdateStat();
  }
  tb->Add(key, value);
}
void DispatcherTableBuilder::UpdateStat() {
  st_sum.Add(st);
  const_cast<DispatcherTableFactory*>(dispatcher)->UpdateStat(level, st);
  PrintLog(5, "TRAC5: DispatcherTableBuilder::UpdateStat: ""entry_cnt = %zd",
            st_sum.entry_cnt);
  st.Reset();
}

void DispatcherTableFactory::UpdateStat(size_t lev, const Stat& st) {
  auto tp = steady_clock::now();
  m_mtx.lock();
  m_stats[0][lev].time = tp;
  m_stats[0][lev].st.Add(st);
  for (size_t i = 1; i < 6; ++i) {
    assert(m_stats[i].size() == m_level_writers.size() + 1);
    assert(lev <= m_level_writers.size());
    auto& ts = m_stats[i][lev];
    PrintLog(6, "TRAC6: DispatcherTableBuilder::UpdateStat: "
"tp-ts.time = %zd, g_durations[i] = %zd, (tp - ts.time > g_durations[i]) = %d\n",
              (size_t)duration_cast<seconds>(tp - ts.time).count(),
              (size_t)duration_cast<seconds>(g_durations[i]).count(),
              tp - ts.time > g_durations[i]);
    if (tp - ts.time > g_durations[i]) {
      auto& newer = m_stats[i-1][lev];
      ts = newer; // refresh
    }
  }
  m_mtx.unlock();
}

static std::shared_ptr<TableFactory>
NewDispatcherTableFactoryJson(const json& js, const SidePluginRepo& repo) {
  return std::make_shared<DispatcherTableFactory>(js, repo);
}

struct DispatcherTableFactory_Manip : PluginManipFunc<TableFactory> {
  void Update(TableFactory* p, const json&, const json& js,
              const SidePluginRepo& repo) const final {
    if (auto t = dynamic_cast<DispatcherTableFactory*>(p)) {
      t->UpdateOptions(js, repo);
      return;
    }
    std::string name = p->Name();
    THROW_InvalidArgument("Is not DispatcherTable, but is: " + name);
  }
  std::string ToString(const TableFactory& fac, const json& dump_options,
                       const SidePluginRepo& repo) const final {
    if (auto t = dynamic_cast<const DispatcherTableFactory*>(&fac)) {
      return t->ToJsonStr(dump_options, repo);
    }
    std::string name = fac.Name();
    THROW_InvalidArgument("Is not DispatcherTable, but is: " + name);
  }
};

ROCKSDB_REG_PluginManip("Dispatch", DispatcherTableFactory_Manip);
ROCKSDB_REG_PluginManip("Dispatcher", DispatcherTableFactory_Manip);
ROCKSDB_REG_PluginManip("DispatcherTable", DispatcherTableFactory_Manip);

ROCKSDB_FACTORY_REG("Dispatch", NewDispatcherTableFactoryJson);
ROCKSDB_FACTORY_REG("Dispatcher", NewDispatcherTableFactoryJson);
ROCKSDB_FACTORY_REG("DispatcherTable", NewDispatcherTableFactoryJson);

void TableFactoryDummyFuncToPreventGccDeleteSymbols() {}

}
