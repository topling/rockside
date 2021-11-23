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

#include "logging/logging.h"
#include "rocksdb/db.h"
#include "db/dbformat.h"
#include "db/column_family.h"
#include "db/table_cache.h"
#include "db/version_set.h"
#include "rocksdb/options.h"
#include "options/db_options.h"
#include "rocksdb/utilities/db_ttl.h"
#include "rocksdb/utilities/transaction_db.h"
#include "rocksdb/utilities/transaction_db_mutex.h"
#include "rocksdb/utilities/optimistic_transaction_db.h"
#include "utilities/blob_db/blob_db.h"
#include "utilities/transactions/transaction_db_mutex_impl.h"
#include "side_plugin_factory.h"
#include "side_plugin_internal.h"

extern const char* rocksdb_build_git_tag;
extern const char* rocksdb_build_git_sha;
extern const char* rocksdb_build_git_date;
extern const char* rocksdb_build_date;

namespace ROCKSDB_NAMESPACE {

using std::shared_ptr;
using std::vector;
using std::string;

template<class T>
static std::ostringstream& operator|(std::ostringstream& oss, const T& x) {
  oss << x;
  return oss;
}
// bucketMapper is defined in histogram.cc
extern const HistogramBucketMapper bucketMapper;

static std::shared_ptr<FileSystem>
DefaultFileSystemForJson(const json&, const SidePluginRepo&) {
  return FileSystem::Default();
}
ROCKSDB_FACTORY_REG("posix", DefaultFileSystemForJson);
ROCKSDB_FACTORY_REG("Posix", DefaultFileSystemForJson);
ROCKSDB_FACTORY_REG("default", DefaultFileSystemForJson);
ROCKSDB_FACTORY_REG("Default", DefaultFileSystemForJson);

static bool IsDefaultPath(const vector<DbPath>& paths, const string& name) {
  if (paths.size() != 1) {
    return false;
  }
  return paths[0].path == name && paths[0].target_size == UINT64_MAX;
}

static json DbPathToJson(const DbPath& x, bool simplify_zero) {
  if (simplify_zero && (0 == x.target_size || UINT64_MAX == x.target_size))
    return json{x.path};
  else
    return json{
        { "path", x.path },
        { "target_size", x.target_size }
    };
}

static json DbPathVecToJson(const std::vector<DbPath>& vec, bool html) {
  json js;
  if (vec.size() == 1) {
    js = DbPathToJson(vec[0], true);
  }
  else if (!vec.empty()) {
    auto is_non_zero = [](const DbPath& p) {return 0 != p.target_size; };
    bool has_non_zero = std::any_of(vec.begin(), vec.end(), is_non_zero);
    for (auto& x : vec) {
      js.push_back(DbPathToJson(x, !has_non_zero));
    }
    if (html && has_non_zero)
      js[0]["<htmltab:col>"] = json::array({ "path", "target_size" });
  }
  return js;
}

static DbPath DbPathFromJson(const json& js) {
  DbPath x;
  if (js.is_string()) {
    x.path = js.get<string>();
  } else {
    x.path = js.at("path").get<string>();
    x.target_size = ParseSizeXiB(js, "target_size");
  }
  return x;
}

static void Json_DbPathVec(const json& js, std::vector<DbPath>& db_paths) {
  db_paths.clear();
  if (js.is_string() || js.is_object()) {
    // only one path
    db_paths.push_back(DbPathFromJson(js));
  }
  else if (js.is_array()) {
    for (auto& one : js.items()) {
      db_paths.push_back(DbPathFromJson(one.value()));
    }
  }
}

static Status Json_EventListenerVec(const json& js, const SidePluginRepo& repo,
                                    std::vector<shared_ptr<EventListener>>& listeners) {
  listeners.clear();
  if (js.is_string()) {
    shared_ptr<EventListener> el;
    ROCKSDB_JSON_OPT_FACT_INNER(js, el);
    listeners.emplace_back(el);
  }
  else if (js.is_array()) {
    for (auto& one : js.items()) {
      shared_ptr<EventListener> el;
      ROCKSDB_JSON_OPT_FACT_INNER(one.value(), el);
      listeners.emplace_back(el);
    }
  }
  return Status::OK();
}

struct DBOptions_Json : DBOptions {
  DBOptions_Json(const json& js, const SidePluginRepo& repo) {
    write_dbid_to_manifest = true;
    avoid_unnecessary_blocking_io = true;
    Update(js, repo);
  }
  void Update(const json& js, const SidePluginRepo& repo) {
    ROCKSDB_JSON_OPT_PROP(js, create_if_missing);
    ROCKSDB_JSON_OPT_PROP(js, create_missing_column_families);
    ROCKSDB_JSON_OPT_PROP(js, error_if_exists);
    ROCKSDB_JSON_OPT_PROP(js, paranoid_checks);
    ROCKSDB_JSON_OPT_PROP(js, track_and_verify_wals_in_manifest);
    ROCKSDB_JSON_OPT_FACT(js, env);
    ROCKSDB_JSON_OPT_FACT(js, rate_limiter);
    ROCKSDB_JSON_OPT_FACT(js, sst_file_manager);
    ROCKSDB_JSON_OPT_FACT(js, info_log);
    ROCKSDB_JSON_OPT_ENUM(js, info_log_level);
    ROCKSDB_JSON_OPT_PROP(js, max_open_files);
    ROCKSDB_JSON_OPT_PROP(js, max_file_opening_threads);
    ROCKSDB_JSON_OPT_SIZE(js, max_total_wal_size);
    ROCKSDB_JSON_OPT_FACT(js, statistics);
    ROCKSDB_JSON_OPT_PROP(js, use_fsync);
    ROCKSDB_JSON_OPT_PROP(js, allow_fdatasync);
    {
      auto iter = js.find("db_paths");
      if (js.end() != iter)
        Json_DbPathVec(iter.value(), db_paths);
    }
    ROCKSDB_JSON_OPT_PROP(js, db_log_dir);
    ROCKSDB_JSON_OPT_PROP(js, wal_dir);
    ROCKSDB_JSON_OPT_PROP(js, delete_obsolete_files_period_micros);
    ROCKSDB_JSON_OPT_PROP(js, max_background_jobs);
    ROCKSDB_JSON_OPT_PROP(js, base_background_compactions);
    ROCKSDB_JSON_OPT_PROP(js, max_background_compactions);
    ROCKSDB_JSON_OPT_PROP(js, max_subcompactions);
    ROCKSDB_JSON_OPT_PROP(js, max_level1_subcompactions);
    ROCKSDB_JSON_OPT_PROP(js, max_background_flushes);
    ROCKSDB_JSON_OPT_SIZE(js, max_log_file_size);
    ROCKSDB_JSON_OPT_PROP(js, log_file_time_to_roll);
    ROCKSDB_JSON_OPT_PROP(js, keep_log_file_num);
    ROCKSDB_JSON_OPT_PROP(js, recycle_log_file_num);
    ROCKSDB_JSON_OPT_SIZE(js, max_manifest_file_size);
    ROCKSDB_JSON_OPT_PROP(js, table_cache_numshardbits);
    ROCKSDB_JSON_OPT_PROP(js, WAL_ttl_seconds);
    ROCKSDB_JSON_OPT_PROP(js, WAL_size_limit_MB);
    ROCKSDB_JSON_OPT_SIZE(js, manifest_preallocation_size);
    ROCKSDB_JSON_OPT_PROP(js, allow_mmap_reads);
    ROCKSDB_JSON_OPT_PROP(js, allow_mmap_writes);
    ROCKSDB_JSON_OPT_PROP(js, use_direct_reads);
    ROCKSDB_JSON_OPT_PROP(js, use_direct_io_for_flush_and_compaction);
    ROCKSDB_JSON_OPT_PROP(js, allow_fallocate);
    ROCKSDB_JSON_OPT_PROP(js, is_fd_close_on_exec);
    ROCKSDB_JSON_OPT_PROP(js, skip_log_error_on_recovery);
    ROCKSDB_JSON_OPT_PROP(js, stats_dump_period_sec);
    ROCKSDB_JSON_OPT_PROP(js, stats_persist_period_sec);
    ROCKSDB_JSON_OPT_PROP(js, persist_stats_to_disk);
    ROCKSDB_JSON_OPT_SIZE(js, stats_history_buffer_size);
    ROCKSDB_JSON_OPT_PROP(js, advise_random_on_open);
    ROCKSDB_JSON_OPT_SIZE(js, db_write_buffer_size);
    ROCKSDB_JSON_OPT_FACT(js, write_buffer_manager);
    ROCKSDB_JSON_OPT_ENUM(js, access_hint_on_compaction_start);
    ROCKSDB_JSON_OPT_PROP(js, new_table_reader_for_compaction_inputs);
    ROCKSDB_JSON_OPT_SIZE(js, compaction_readahead_size);
    ROCKSDB_JSON_OPT_SIZE(js, random_access_max_buffer_size);
    ROCKSDB_JSON_OPT_SIZE(js, writable_file_max_buffer_size);
    ROCKSDB_JSON_OPT_PROP(js, use_adaptive_mutex);
    ROCKSDB_JSON_OPT_SIZE(js, bytes_per_sync);
    ROCKSDB_JSON_OPT_SIZE(js, wal_bytes_per_sync);
    ROCKSDB_JSON_OPT_PROP(js, strict_bytes_per_sync);
    {
      auto iter = js.find("listeners");
      if (js.end() != iter)
        Json_EventListenerVec(js, repo, listeners);
    }
    ROCKSDB_JSON_OPT_PROP(js, enable_thread_tracking);
    ROCKSDB_JSON_OPT_SIZE(js, delayed_write_rate);
    ROCKSDB_JSON_OPT_PROP(js, enable_pipelined_write);
    ROCKSDB_JSON_OPT_PROP(js, unordered_write);
    ROCKSDB_JSON_OPT_PROP(js, allow_concurrent_memtable_write);
    ROCKSDB_JSON_OPT_PROP(js, enable_write_thread_adaptive_yield);
    ROCKSDB_JSON_OPT_SIZE(js, max_write_batch_group_size_bytes);
    ROCKSDB_JSON_OPT_PROP(js, write_thread_max_yield_usec);
    ROCKSDB_JSON_OPT_PROP(js, write_thread_slow_yield_usec);
    ROCKSDB_JSON_OPT_PROP(js, skip_stats_update_on_db_open);
    ROCKSDB_JSON_OPT_PROP(js, skip_checking_sst_file_sizes_on_db_open);
    ROCKSDB_JSON_OPT_ENUM(js, wal_recovery_mode);
    ROCKSDB_JSON_OPT_PROP(js, allow_2pc);
    ROCKSDB_JSON_OPT_FACT(js, row_cache);
    //ROCKSDB_JSON_OPT_FACT(js, wal_filter);
    ROCKSDB_JSON_OPT_PROP(js, fail_if_options_file_error);
    ROCKSDB_JSON_OPT_PROP(js, dump_malloc_stats);
    ROCKSDB_JSON_OPT_PROP(js, avoid_flush_during_recovery);
    ROCKSDB_JSON_OPT_PROP(js, avoid_flush_during_shutdown);
    ROCKSDB_JSON_OPT_PROP(js, allow_ingest_behind);
    ROCKSDB_JSON_OPT_PROP(js, preserve_deletes);
    ROCKSDB_JSON_OPT_PROP(js, two_write_queues);
    ROCKSDB_JSON_OPT_PROP(js, manual_wal_flush);
    ROCKSDB_JSON_OPT_PROP(js, atomic_flush);
    ROCKSDB_JSON_OPT_PROP(js, avoid_unnecessary_blocking_io);
    ROCKSDB_JSON_OPT_PROP(js, write_dbid_to_manifest);
    ROCKSDB_JSON_OPT_SIZE(js, log_readahead_size);
    ROCKSDB_JSON_OPT_FACT(js, file_checksum_gen_factory);
    ROCKSDB_JSON_OPT_PROP(js, best_efforts_recovery);
    ROCKSDB_JSON_OPT_PROP(js, max_bgerror_resume_count);
    ROCKSDB_JSON_OPT_PROP(js, bgerror_resume_retry_interval);
    ROCKSDB_JSON_OPT_PROP(js, allow_data_in_errors);
    ROCKSDB_JSON_OPT_PROP(js, db_host_id);
    //ROCKSDB_JSON_OPT_ESET(js, checksum_handoff_file_types); // EnumSet
  }

  void SaveToJson(json& js, const SidePluginRepo& repo, bool html) const {
    ROCKSDB_JSON_SET_PROP(js, paranoid_checks);
    ROCKSDB_JSON_SET_PROP(js, track_and_verify_wals_in_manifest);
    ROCKSDB_JSON_SET_FACT(js, env);
    ROCKSDB_JSON_SET_FACT(js, rate_limiter);
    ROCKSDB_JSON_SET_FACT(js, sst_file_manager);
    ROCKSDB_JSON_SET_FACT(js, info_log);
    ROCKSDB_JSON_SET_ENUM(js, info_log_level);
    ROCKSDB_JSON_SET_PROP(js, max_open_files);
    ROCKSDB_JSON_SET_PROP(js, max_file_opening_threads);
    ROCKSDB_JSON_SET_SIZE(js, max_total_wal_size);
    ROCKSDB_JSON_SET_FACT(js, statistics);
    ROCKSDB_JSON_SET_PROP(js, use_fsync);
    ROCKSDB_JSON_SET_PROP(js, allow_fdatasync);
    if (!db_paths.empty()) {
      js["db_paths"] = DbPathVecToJson(db_paths, html);
    }
    ROCKSDB_JSON_SET_PROP(js, db_log_dir);
    ROCKSDB_JSON_SET_PROP(js, wal_dir);
    ROCKSDB_JSON_SET_PROP(js, delete_obsolete_files_period_micros);
    ROCKSDB_JSON_SET_PROP(js, max_background_jobs);
    ROCKSDB_JSON_SET_PROP(js, base_background_compactions);
    ROCKSDB_JSON_SET_PROP(js, max_background_compactions);
    ROCKSDB_JSON_SET_PROP(js, max_subcompactions);
    ROCKSDB_JSON_SET_PROP(js, max_level1_subcompactions);
    ROCKSDB_JSON_SET_PROP(js, max_background_flushes);
    ROCKSDB_JSON_SET_SIZE(js, max_log_file_size);
    ROCKSDB_JSON_SET_PROP(js, log_file_time_to_roll);
    ROCKSDB_JSON_SET_PROP(js, keep_log_file_num);
    ROCKSDB_JSON_SET_PROP(js, recycle_log_file_num);
    ROCKSDB_JSON_SET_SIZE(js, max_manifest_file_size);
    ROCKSDB_JSON_SET_PROP(js, table_cache_numshardbits);
    ROCKSDB_JSON_SET_PROP(js, WAL_ttl_seconds);
    ROCKSDB_JSON_SET_PROP(js, WAL_size_limit_MB);
    ROCKSDB_JSON_SET_SIZE(js, manifest_preallocation_size);
    ROCKSDB_JSON_SET_PROP(js, allow_mmap_reads);
    ROCKSDB_JSON_SET_PROP(js, allow_mmap_writes);
    ROCKSDB_JSON_SET_PROP(js, use_direct_reads);
    ROCKSDB_JSON_SET_PROP(js, use_direct_io_for_flush_and_compaction);
    ROCKSDB_JSON_SET_PROP(js, allow_fallocate);
    ROCKSDB_JSON_SET_PROP(js, is_fd_close_on_exec);
    ROCKSDB_JSON_SET_PROP(js, skip_log_error_on_recovery);
    ROCKSDB_JSON_SET_PROP(js, stats_dump_period_sec);
    ROCKSDB_JSON_SET_PROP(js, stats_persist_period_sec);
    ROCKSDB_JSON_SET_PROP(js, persist_stats_to_disk);
    ROCKSDB_JSON_SET_SIZE(js, stats_history_buffer_size);
    ROCKSDB_JSON_SET_PROP(js, advise_random_on_open);
    ROCKSDB_JSON_SET_SIZE(js, db_write_buffer_size);
    ROCKSDB_JSON_SET_FACT(js, write_buffer_manager);
    ROCKSDB_JSON_SET_ENUM(js, access_hint_on_compaction_start);
    ROCKSDB_JSON_SET_PROP(js, new_table_reader_for_compaction_inputs);
    ROCKSDB_JSON_SET_SIZE(js, compaction_readahead_size);
    ROCKSDB_JSON_SET_SIZE(js, random_access_max_buffer_size);
    ROCKSDB_JSON_SET_SIZE(js, writable_file_max_buffer_size);
    ROCKSDB_JSON_SET_PROP(js, use_adaptive_mutex);
    ROCKSDB_JSON_SET_SIZE(js, bytes_per_sync);
    ROCKSDB_JSON_SET_SIZE(js, wal_bytes_per_sync);
    ROCKSDB_JSON_SET_PROP(js, strict_bytes_per_sync);
    for (auto& listener : listeners) {
      json inner;
      ROCKSDB_JSON_SET_FACT_INNER(inner, listener, event_listener);
      js["listeners"].push_back(inner);
    }
    ROCKSDB_JSON_SET_PROP(js, enable_thread_tracking);
    ROCKSDB_JSON_SET_SIZE(js, delayed_write_rate);
    ROCKSDB_JSON_SET_PROP(js, enable_pipelined_write);
    ROCKSDB_JSON_SET_PROP(js, unordered_write);
    ROCKSDB_JSON_SET_PROP(js, allow_concurrent_memtable_write);
    ROCKSDB_JSON_SET_PROP(js, enable_write_thread_adaptive_yield);
    ROCKSDB_JSON_SET_SIZE(js, max_write_batch_group_size_bytes);
    ROCKSDB_JSON_SET_PROP(js, write_thread_max_yield_usec);
    ROCKSDB_JSON_SET_PROP(js, write_thread_slow_yield_usec);
    ROCKSDB_JSON_SET_PROP(js, skip_stats_update_on_db_open);
    ROCKSDB_JSON_SET_PROP(js, skip_checking_sst_file_sizes_on_db_open);
    ROCKSDB_JSON_SET_ENUM(js, wal_recovery_mode);
    ROCKSDB_JSON_SET_PROP(js, allow_2pc);
    ROCKSDB_JSON_SET_FACX(js, row_cache, cache);
    //ROCKSDB_JSON_SET_FACT(js, wal_filter);
    ROCKSDB_JSON_SET_PROP(js, fail_if_options_file_error);
    ROCKSDB_JSON_SET_PROP(js, dump_malloc_stats);
    ROCKSDB_JSON_SET_PROP(js, avoid_flush_during_recovery);
    ROCKSDB_JSON_SET_PROP(js, avoid_flush_during_shutdown);
    ROCKSDB_JSON_SET_PROP(js, allow_ingest_behind);
    ROCKSDB_JSON_SET_PROP(js, preserve_deletes);
    ROCKSDB_JSON_SET_PROP(js, two_write_queues);
    ROCKSDB_JSON_SET_PROP(js, manual_wal_flush);
    ROCKSDB_JSON_SET_PROP(js, atomic_flush);
    ROCKSDB_JSON_SET_PROP(js, avoid_unnecessary_blocking_io);
    ROCKSDB_JSON_SET_PROP(js, write_dbid_to_manifest);
    ROCKSDB_JSON_SET_SIZE(js, log_readahead_size);
    ROCKSDB_JSON_SET_FACT(js, file_checksum_gen_factory);
    ROCKSDB_JSON_SET_PROP(js, best_efforts_recovery);
    ROCKSDB_JSON_SET_PROP(js, max_bgerror_resume_count);
    ROCKSDB_JSON_SET_PROP(js, bgerror_resume_retry_interval);
    ROCKSDB_JSON_SET_PROP(js, allow_data_in_errors);
    ROCKSDB_JSON_SET_PROP(js, db_host_id);
    //ROCKSDB_JSON_SET_ESET(js, checksum_handoff_file_types); // EnumSet
  }
};
ROCKSDB_REG_JSON_REPO_CONS("DBOptions", DBOptions_Json, DBOptions);
struct DBOptions_Manip : PluginManipFunc<DBOptions> {
  void Update(DBOptions* p, const json& js, const SidePluginRepo& repo)
  const final {
    auto iter = js.find("objname");
    if (js.end() == iter) {
      THROW_Corruption("missing js[\"objname\"]");
    }
    //auto& objname = iter.value().get_ref<const std::string&>();
    //std::shared_ptr<DBOptions> dbo = repo[objname];

    static_cast<DBOptions_Json*>(p)->Update(js, repo);
  }
  std::string ToString(const DBOptions& x, const json& dump_options,
                       const SidePluginRepo& repo) const final {
    json djs;
    bool html = JsonSmartBool(dump_options, "html", true);
    static_cast<const DBOptions_Json&>(x).SaveToJson(djs, repo, html);
    return JsonToString(djs, dump_options);
  }
};
ROCKSDB_REG_PluginManip("DBOptions", DBOptions_Manip);

///////////////////////////////////////////////////////////////////////////
template<class Vec>
bool Init_vec(const json& js, Vec& vec) {
  if (js.is_array()) {
    vec.clear();
    for (auto& iv : js.items()) {
      vec.push_back(iv.value().get<typename Vec::value_type>());
    }
    return true;
  } else {
    return false;
  }
}

//NLOHMANN_DEFINE_TYPE_NON_INTRUSIVE(CompactionOptionsFIFO,
//                                   max_table_files_size,
//                                   allow_compaction);

struct CompactionOptionsFIFO_Json : CompactionOptionsFIFO {
  explicit CompactionOptionsFIFO_Json(const json& js) {
    ROCKSDB_JSON_OPT_PROP(js, max_table_files_size);
    ROCKSDB_JSON_OPT_PROP(js, allow_compaction);
  }
  void SaveToJson(json& js) const {
    ROCKSDB_JSON_SET_PROP(js, max_table_files_size);
    ROCKSDB_JSON_SET_PROP(js, allow_compaction);
  }
};
CompactionOptionsFIFO_Json NestForBase(const CompactionOptionsFIFO&);

struct CompactionOptionsUniversal_Json : CompactionOptionsUniversal {
  explicit CompactionOptionsUniversal_Json(const json& js) {
    ROCKSDB_JSON_OPT_PROP(js, size_ratio);
    ROCKSDB_JSON_OPT_PROP(js, min_merge_width);
    ROCKSDB_JSON_OPT_PROP(js, max_merge_width);
    ROCKSDB_JSON_OPT_PROP(js, max_size_amplification_percent);
    ROCKSDB_JSON_OPT_PROP(js, compression_size_percent);
    ROCKSDB_JSON_OPT_ENUM(js, stop_style);
    ROCKSDB_JSON_OPT_PROP(js, allow_trivial_move);
  }
  void SaveToJson(json& js) const {
    ROCKSDB_JSON_SET_PROP(js, size_ratio);
    ROCKSDB_JSON_SET_PROP(js, min_merge_width);
    ROCKSDB_JSON_SET_PROP(js, max_merge_width);
    ROCKSDB_JSON_SET_PROP(js, max_size_amplification_percent);
    ROCKSDB_JSON_SET_PROP(js, compression_size_percent);
    ROCKSDB_JSON_SET_ENUM(js, stop_style);
    ROCKSDB_JSON_SET_PROP(js, allow_trivial_move);
  }
};
CompactionOptionsUniversal_Json NestForBase(const CompactionOptionsUniversal&);

struct CompressionOptions_Json : CompressionOptions {
  explicit CompressionOptions_Json(const json& js) {
    ROCKSDB_JSON_OPT_PROP(js, window_bits);
    ROCKSDB_JSON_OPT_PROP(js, level);
    ROCKSDB_JSON_OPT_PROP(js, strategy);
    ROCKSDB_JSON_OPT_SIZE(js, max_dict_bytes);
    ROCKSDB_JSON_OPT_PROP(js, zstd_max_train_bytes);
    ROCKSDB_JSON_OPT_PROP(js, parallel_threads);
    ROCKSDB_JSON_OPT_PROP(js, enabled);
  }
  void SaveToJson(json& js) const {
    ROCKSDB_JSON_SET_PROP(js, window_bits);
    ROCKSDB_JSON_SET_PROP(js, level);
    ROCKSDB_JSON_SET_PROP(js, strategy);
    ROCKSDB_JSON_SET_SIZE(js, max_dict_bytes);
    ROCKSDB_JSON_SET_PROP(js, zstd_max_train_bytes);
    ROCKSDB_JSON_SET_PROP(js, parallel_threads);
    ROCKSDB_JSON_SET_PROP(js, enabled);
  }
};
CompressionOptions_Json NestForBase(const CompressionOptions&);

struct ColumnFamilyOptions_Json : ColumnFamilyOptions {
  ColumnFamilyOptions_Json(const json& js, const SidePluginRepo& repo) {
    Update(js, repo);
  }
  void Update(const json& js, const SidePluginRepo& repo) {
    ROCKSDB_JSON_OPT_PROP(js, max_write_buffer_number);
    ROCKSDB_JSON_OPT_PROP(js, min_write_buffer_number_to_merge);
    ROCKSDB_JSON_OPT_PROP(js, max_write_buffer_number_to_maintain);
    ROCKSDB_JSON_OPT_SIZE(js, max_write_buffer_size_to_maintain);
    ROCKSDB_JSON_OPT_PROP(js, inplace_update_support);
    ROCKSDB_JSON_OPT_PROP(js, inplace_update_num_locks);
 // ROCKSDB_JSON_OPT_PROP(js, inplace_callback); // not need update
    ROCKSDB_JSON_OPT_PROP(js, memtable_prefix_bloom_size_ratio);
    ROCKSDB_JSON_OPT_PROP(js, memtable_whole_key_filtering);
    ROCKSDB_JSON_OPT_PROP(js, memtable_huge_page_size);
    ROCKSDB_JSON_OPT_FACT(js, memtable_insert_with_hint_prefix_extractor);
    ROCKSDB_JSON_OPT_PROP(js, bloom_locality);
    ROCKSDB_JSON_OPT_SIZE(js, arena_block_size);
    { // compression_per_level is an enum array
      auto iter = js.find("compression_per_level");
      if (js.end() != iter) {
        if (!iter.value().is_array()) {
          THROW_InvalidArgument("compression_per_level must be an array");
        }
        compression_per_level.resize(0);
        for (auto& item : iter.value().items()) {
          const string& val = item.value().get_ref<const string&>();
          CompressionType compressionType;
          if (!enum_value(val, &compressionType)) {
            THROW_InvalidArgument("compression_per_level: invalid enum: " + val);
          }
          compression_per_level.push_back(compressionType);
        }
      }
    }
    ROCKSDB_JSON_OPT_PROP(js, num_levels);
    ROCKSDB_JSON_OPT_PROP(js, level0_slowdown_writes_trigger);
    ROCKSDB_JSON_OPT_PROP(js, level0_stop_writes_trigger);
    ROCKSDB_JSON_OPT_SIZE(js, target_file_size_base);
    ROCKSDB_JSON_OPT_PROP(js, target_file_size_multiplier);
    ROCKSDB_JSON_OPT_PROP(js, level_compaction_dynamic_level_bytes);
    ROCKSDB_JSON_OPT_PROP(js, max_bytes_for_level_multiplier);
    {
      auto iter = js.find("max_bytes_for_level_multiplier_additional");
      if (js.end() != iter)
        if (!Init_vec(iter.value(), max_bytes_for_level_multiplier_additional))
          THROW_InvalidArgument(
              "max_bytes_for_level_multiplier_additional must be an int vector");
    }
    ROCKSDB_JSON_OPT_SIZE(js, max_compaction_bytes);
    ROCKSDB_JSON_OPT_SIZE(js, soft_pending_compaction_bytes_limit);
    ROCKSDB_JSON_OPT_SIZE(js, hard_pending_compaction_bytes_limit);
    ROCKSDB_JSON_OPT_ENUM(js, compaction_style);
    ROCKSDB_JSON_OPT_PROP(js, compaction_pri);
    ROCKSDB_JSON_OPT_NEST(js, compaction_options_universal);
    ROCKSDB_JSON_OPT_NEST(js, compaction_options_fifo);
    ROCKSDB_JSON_OPT_PROP(js, max_sequential_skip_in_iterations);
    ROCKSDB_JSON_OPT_FACT(js, memtable_factory);
    {
      auto iter = js.find("table_properties_collector_factories");
      if (js.end() != iter) {
        if (!iter.value().is_array()) {
          THROW_InvalidArgument(
              "table_properties_collector_factories must be an array");
        }
        decltype(table_properties_collector_factories) vec;
        for (auto& item : iter.value().items()) {
          decltype(vec)::value_type p;
          ROCKSDB_JSON_OPT_FACT_INNER(item.value(), p);
          vec.push_back(p);
        }
        table_properties_collector_factories.swap(vec);
      }
    }
    ROCKSDB_JSON_OPT_PROP(js, max_successive_merges);
    ROCKSDB_JSON_OPT_PROP(js, optimize_filters_for_hits);
    ROCKSDB_JSON_OPT_PROP(js, paranoid_file_checks);
    ROCKSDB_JSON_OPT_PROP(js, force_consistency_checks);
    ROCKSDB_JSON_OPT_PROP(js, report_bg_io_stats);
    ROCKSDB_JSON_OPT_PROP(js, ttl);
    ROCKSDB_JSON_OPT_PROP(js, periodic_compaction_seconds);
    ROCKSDB_JSON_OPT_PROP(js, sample_for_compression);
    ROCKSDB_JSON_OPT_PROP(js, max_mem_compaction_level);
    ROCKSDB_JSON_OPT_PROP(js, soft_rate_limit);
    ROCKSDB_JSON_OPT_PROP(js, hard_rate_limit);
    ROCKSDB_JSON_OPT_PROP(js, rate_limit_delay_max_milliseconds);
    ROCKSDB_JSON_OPT_PROP(js, purge_redundant_kvs_while_flush);
    // ------- ColumnFamilyOptions specific --------------------------
    ROCKSDB_JSON_OPT_FACT(js, comparator);
    ROCKSDB_JSON_OPT_FACT(js, merge_operator);
    // ROCKSDB_JSON_OPT_FACT(js, compaction_filter);
    ROCKSDB_JSON_OPT_FACT(js, compaction_filter_factory);
    ROCKSDB_JSON_OPT_SIZE(js, write_buffer_size);
    ROCKSDB_JSON_OPT_ENUM(js, compression);
    ROCKSDB_JSON_OPT_ENUM(js, bottommost_compression);
    ROCKSDB_JSON_OPT_NEST(js, bottommost_compression_opts);
    ROCKSDB_JSON_OPT_NEST(js, compression_opts);
    ROCKSDB_JSON_OPT_PROP(js, level0_file_num_compaction_trigger);
    ROCKSDB_JSON_OPT_FACT(js, prefix_extractor);
    ROCKSDB_JSON_OPT_SIZE(js, max_bytes_for_level_base);
    ROCKSDB_JSON_OPT_PROP(js, snap_refresh_nanos);
    ROCKSDB_JSON_OPT_PROP(js, disable_auto_compactions);
    ROCKSDB_JSON_OPT_FACT(js, table_factory);
    {
      auto iter = js.find("cf_paths");
      if (js.end() != iter) Json_DbPathVec(iter.value(), cf_paths);
    }
    ROCKSDB_JSON_OPT_FACT(js, compaction_thread_limiter);
    ROCKSDB_JSON_OPT_FACT(js, sst_partitioner_factory);
    ROCKSDB_JSON_OPT_FACT(js, compaction_executor_factory);
    ROCKSDB_JSON_OPT_FACT(js, html_user_key_coder);
    if (html_user_key_coder) {
      if (!dynamic_cast<UserKeyCoder*>(html_user_key_coder.get())) {
        THROW_InvalidArgument("bad html_user_key_coder = %s" + js["html_user_key_coder"].dump());
      }
    }
    if (kCompactionStyleLevel == compaction_style) {
      auto iter = js.find("compaction_options_level");
      if (js.end() != iter) {
        auto L1_score_boost = compaction_options_universal.size_ratio;
        auto& sub = iter.value();
        ROCKSDB_JSON_OPT_PROP(sub, L1_score_boost);
        compaction_options_universal.size_ratio = L1_score_boost;
      }
    }
  }

  void SaveToJson(json& js, const SidePluginRepo& repo, bool html) const {
    ROCKSDB_JSON_SET_PROP(js, max_write_buffer_number);
    ROCKSDB_JSON_SET_PROP(js, min_write_buffer_number_to_merge);
    ROCKSDB_JSON_SET_PROP(js, max_write_buffer_number_to_maintain);
    ROCKSDB_JSON_SET_SIZE(js, max_write_buffer_size_to_maintain);
    ROCKSDB_JSON_SET_PROP(js, inplace_update_support);
    ROCKSDB_JSON_SET_PROP(js, inplace_update_num_locks);
 // ROCKSDB_JSON_SET_PROP(js, inplace_callback); // not need update
    ROCKSDB_JSON_SET_PROP(js, memtable_prefix_bloom_size_ratio);
    ROCKSDB_JSON_SET_PROP(js, memtable_whole_key_filtering);
    ROCKSDB_JSON_SET_PROP(js, memtable_huge_page_size);
    ROCKSDB_JSON_SET_FACX(js, memtable_insert_with_hint_prefix_extractor,
                          slice_transform);
    ROCKSDB_JSON_SET_PROP(js, bloom_locality);
    ROCKSDB_JSON_SET_SIZE(js, arena_block_size);
    auto& js_compression_per_level = js["compression_per_level"];
    js_compression_per_level.clear();
    for (auto one_enum : compression_per_level)
      js_compression_per_level.push_back(enum_stdstr(one_enum));
    ROCKSDB_JSON_SET_PROP(js, num_levels);
    ROCKSDB_JSON_SET_PROP(js, level0_slowdown_writes_trigger);
    ROCKSDB_JSON_SET_PROP(js, level0_stop_writes_trigger);
    ROCKSDB_JSON_SET_SIZE(js, target_file_size_base);
    ROCKSDB_JSON_SET_PROP(js, target_file_size_multiplier);
    ROCKSDB_JSON_SET_PROP(js, level_compaction_dynamic_level_bytes);
    ROCKSDB_JSON_SET_PROP(js, max_bytes_for_level_multiplier);
    ROCKSDB_JSON_SET_PROP(js, max_bytes_for_level_multiplier_additional);
    ROCKSDB_JSON_SET_SIZE(js, max_compaction_bytes);
    ROCKSDB_JSON_SET_SIZE(js, soft_pending_compaction_bytes_limit);
    ROCKSDB_JSON_SET_SIZE(js, hard_pending_compaction_bytes_limit);
    ROCKSDB_JSON_SET_ENUM(js, compaction_style);
    ROCKSDB_JSON_SET_PROP(js, compaction_pri);
    if (kCompactionStyleUniversal == compaction_style) {
      ROCKSDB_JSON_SET_NEST(js, compaction_options_universal);
    } else if (kCompactionStyleLevel == compaction_style) {
      auto L1_score_boost = compaction_options_universal.size_ratio;
      ROCKSDB_JSON_SET_PROP(js["compaction_options_level"], L1_score_boost);
    } else if (kCompactionStyleFIFO == compaction_style) {
      ROCKSDB_JSON_SET_NEST(js, compaction_options_fifo);
    }
    ROCKSDB_JSON_SET_PROP(js, max_sequential_skip_in_iterations);
    ROCKSDB_JSON_SET_FACT(js, memtable_factory);
    js["table_properties_collector_factories"].clear();
    for (auto& table_properties_collector_factory :
               table_properties_collector_factories) {
      json inner;
      ROCKSDB_JSON_SET_FACT_INNER(inner,
                                  table_properties_collector_factory,
                                  table_properties_collector_factory);
      js["table_properties_collector_factories"].push_back(inner);
    }
    ROCKSDB_JSON_SET_PROP(js, max_successive_merges);
    ROCKSDB_JSON_SET_PROP(js, optimize_filters_for_hits);
    ROCKSDB_JSON_SET_PROP(js, paranoid_file_checks);
    ROCKSDB_JSON_SET_PROP(js, force_consistency_checks);
    ROCKSDB_JSON_SET_PROP(js, report_bg_io_stats);
    ROCKSDB_JSON_SET_PROP(js, ttl);
    ROCKSDB_JSON_SET_PROP(js, periodic_compaction_seconds);
    ROCKSDB_JSON_SET_PROP(js, sample_for_compression);
    ROCKSDB_JSON_SET_PROP(js, max_mem_compaction_level);
    ROCKSDB_JSON_SET_PROP(js, soft_rate_limit);
    ROCKSDB_JSON_SET_PROP(js, hard_rate_limit);
    ROCKSDB_JSON_SET_PROP(js, rate_limit_delay_max_milliseconds);
    ROCKSDB_JSON_SET_PROP(js, purge_redundant_kvs_while_flush);
    // ------- ColumnFamilyOptions specific --------------------------
    ROCKSDB_JSON_SET_FACT(js, comparator);
    ROCKSDB_JSON_SET_FACT(js, merge_operator);
    // ROCKSDB_JSON_OPT_FACT(js, compaction_filter);
    ROCKSDB_JSON_SET_FACT(js, compaction_filter_factory);
    ROCKSDB_JSON_SET_SIZE(js, write_buffer_size);
    ROCKSDB_JSON_SET_ENUM(js, compression);
    ROCKSDB_JSON_SET_ENUM(js, bottommost_compression);
    ROCKSDB_JSON_SET_NEST(js, bottommost_compression_opts);
    ROCKSDB_JSON_SET_NEST(js, compression_opts);
    ROCKSDB_JSON_SET_PROP(js, level0_file_num_compaction_trigger);
    ROCKSDB_JSON_SET_FACX(js, prefix_extractor, slice_transform);
    ROCKSDB_JSON_SET_SIZE(js, max_bytes_for_level_base);
    ROCKSDB_JSON_SET_PROP(js, snap_refresh_nanos);
    ROCKSDB_JSON_SET_PROP(js, disable_auto_compactions);
    ROCKSDB_JSON_SET_FACT(js, table_factory);
    if (!cf_paths.empty()) {
      js["cf_paths"] = DbPathVecToJson(cf_paths, html);
    }
    ROCKSDB_JSON_SET_FACT(js, compaction_thread_limiter);
    ROCKSDB_JSON_SET_FACT(js, sst_partitioner_factory);
    ROCKSDB_JSON_SET_FACT(js, compaction_executor_factory);
    ROCKSDB_JSON_SET_FACX(js, html_user_key_coder, any_plugin);
  }
};
using CFOptions = ColumnFamilyOptions;
using CFOptions_Json = ColumnFamilyOptions_Json;

ROCKSDB_REG_JSON_REPO_CONS("ColumnFamilyOptions", CFOptions_Json, CFOptions);
ROCKSDB_REG_JSON_REPO_CONS("CFOptions", CFOptions_Json, CFOptions);
struct CFOptions_Manip : PluginManipFunc<ColumnFamilyOptions> {
  void Update(ColumnFamilyOptions* p, const json& js,
              const SidePluginRepo& repo) const final {
    static_cast<ColumnFamilyOptions_Json*>(p)->Update(js, repo); // NOLINT
  }
  std::string ToString(const ColumnFamilyOptions& x, const json& dump_options,
                       const SidePluginRepo& repo) const final {
    json djs;
    bool html = JsonSmartBool(dump_options, "html", true);
    // NOLINTNEXTLINE
    static_cast<const ColumnFamilyOptions_Json&>(x).SaveToJson(djs, repo, html);
    return JsonToString(djs, dump_options);
  }
};
ROCKSDB_REG_PluginManip("ColumnFamilyOptions", CFOptions_Manip);
ROCKSDB_REG_PluginManip("CFOptions", CFOptions_Manip);

//////////////////////////////////////////////////////////////////////////////

struct RawStatisticsData {
  uint64_t tickers[INTERNAL_TICKER_ENUM_MAX] = {0};
  HistogramStat histograms[INTERNAL_HISTOGRAM_ENUM_MAX];
};

class StatisticsWithDiscards : public StatisticsImpl {
public:
  using StatisticsImpl::StatisticsImpl;
  std::bitset<INTERNAL_TICKER_ENUM_MAX>    m_discard_tickers;
  std::bitset<INTERNAL_HISTOGRAM_ENUM_MAX> m_discard_histograms;
};

#define g_tickers_name_to_val g_tickers_name_to_val_func()
const auto& g_tickers_name_to_val_func() {
  static const auto scm = []() {
    std::map<std::string, uint32_t> m;
    for (const auto& kv : TickersNameMap) {
      m[kv.second] = kv.first;
    }
    return m;
  }();
  return scm;
}
#define g_histograms_name_to_val g_histograms_name_to_val_func()
const auto& g_histograms_name_to_val_func() {
  static const auto scm = []() {
    std::map<std::string, uint32_t> m;
    for (const auto& kv : HistogramsNameMap) {
      m[kv.second] = kv.first;
    }
    return m;
  }();
  return scm;
}

#define ROCKSDB_JSON_GET_FACT_INNER(js, prop) \
    prop = PluginFactory<decltype(prop)>:: \
        GetPlugin(#prop, ROCKSDB_FUNC, js, repo)

static bool is_in_namespace(Slice name, Slice ns) {
  if (name.size() == ns.size()) {
    return memcmp(name.data_, ns.data_, ns.size_) == 0;
  }
  if (name.size() < ns.size()) {
    return false;
  }
  if (name[ns.size()] == '.') {
    return name.starts_with(ns);
  }
  return false;
}

static shared_ptr<Statistics>
JS_NewStatistics(const json& js, const SidePluginRepo& repo) {
  StatsLevel stats_level = kExceptDetailedTimers;
  ROCKSDB_JSON_OPT_ENUM(js, stats_level);
  //auto p = CreateDBStatistics();
  auto p = std::make_shared<StatisticsWithDiscards>(nullptr);
  p->set_stats_level(stats_level);
  auto iter = js.find("discard_tickers");
  if (js.end() != iter) {
    const json& discard = iter.value();
    if (discard.is_string()) {
      std::shared_ptr<Statistics> discard_tickers;
      ROCKSDB_JSON_GET_FACT_INNER(discard, discard_tickers);
      auto q = dynamic_cast<StatisticsWithDiscards*>(discard_tickers.get());
      ROCKSDB_VERIFY(nullptr != q);
      p->m_discard_tickers = q->m_discard_tickers;
    }
    else if (discard.is_array()) {
      for (auto& item : discard.items()) {
        const json& val = item.value();
        if (!val.is_string()) {
          THROW_InvalidArgument("discard_tickers[*] must be string");
        }
        auto& ticker_ns = val.get_ref<const std::string&>();
        if (Slice(ticker_ns).starts_with("//")) {
          continue; // skip comments
        }
        if (Slice(ticker_ns).starts_with("#")) {
          continue; // skip comments
        }
        auto i = g_tickers_name_to_val.lower_bound(ticker_ns);
        if (g_tickers_name_to_val.end() == i) {
          fprintf(stderr, "ERROR: JS_NewStatistics: bad ticker name: %s\n",
                  ticker_ns.c_str());
          continue;
        }
        while (g_tickers_name_to_val.end() != i &&
               is_in_namespace(i->first, ticker_ns)) {
          ROCKSDB_VERIFY_LT(i->second, TICKER_ENUM_MAX);
          p->m_discard_tickers.set(i->second, true);
          ++i;
        }
      }
    }
    else {
        THROW_InvalidArgument("discard_tickers must be string or array");
    }
  }
  iter = js.find("discard_histograms");
  if (js.end() != iter) {
    const json& discard = iter.value();
    if (discard.is_string()) {
      std::shared_ptr<Statistics> discard_histograms;
      ROCKSDB_JSON_GET_FACT_INNER(discard, discard_histograms);
      auto q = dynamic_cast<StatisticsWithDiscards*>(discard_histograms.get());
      ROCKSDB_VERIFY(nullptr != q);
      p->m_discard_histograms = q->m_discard_histograms;
    }
    else if (discard.is_array()) {
      for (auto& item : discard.items()) {
        const json& val = item.value();
        if (!val.is_string()) {
          THROW_InvalidArgument("discard_histograms[*] must be string");
        }
        auto& histogram_ns = val.get_ref<const std::string&>();
        if (Slice(histogram_ns).starts_with("//")) {
          continue; // skip comments
        }
        if (Slice(histogram_ns).starts_with("#")) {
          continue; // skip comments
        }
        auto i = g_histograms_name_to_val.lower_bound(histogram_ns);
        if (g_histograms_name_to_val.end() == i) {
          fprintf(stderr, "ERROR: JS_NewStatistics: bad histogram name: %s\n",
                  histogram_ns.c_str());
          continue;
        }
        while (g_histograms_name_to_val.end() != i &&
               is_in_namespace(i->first, histogram_ns)) {
          ROCKSDB_VERIFY_LT(i->second, HISTOGRAM_ENUM_MAX);
          p->m_discard_histograms.set(i->second, true);
          ++i;
        }
      }
    }
    else {
        THROW_InvalidArgument("discard_histograms must be string or array");
    }
  }
  return p;
}
ROCKSDB_FACTORY_REG("default", JS_NewStatistics);
ROCKSDB_FACTORY_REG("Default", JS_NewStatistics);
ROCKSDB_FACTORY_REG("Statistics", JS_NewStatistics);

//////////////////////////////////////////////////////////////////////////////

/////////////////////////////////////////////////////////////////////////////
// OpenDB implementations
//

template<class Ptr>
typename std::map<std::string, Ptr>::iterator
IterPluginFind(SidePluginRepo::Impl::ObjMap<Ptr>& field, const std::string& str) {
  if ('$' != str[0]) {
    auto iter = field.name2p->find(str);
    if (field.name2p->end() == iter) {
        THROW_NotFound("class/inst_id = \"" + str + "\"");
    }
    return iter;
  }
  else {
    const std::string inst_id = PluginParseInstID(str);
    auto iter = field.name2p->find(inst_id);
    if (field.name2p->end() == iter) {
        THROW_NotFound("inst_id = \"" + inst_id + "\"");
    }
    return iter;
  }
}

const char* db_options_class = "DBOptions";
const char* cf_options_class = "CFOptions";
template<class Ptr>
Ptr ObtainOPT(SidePluginRepo::Impl::ObjMap<Ptr>& field,
              const char* option_class, // "DBOptions" or "CFOptions"
              const json& option_js, const SidePluginRepo& repo) {
  auto& name2p = *field.name2p;
  if (option_js.is_string()) {
    const std::string& option_name = option_js.get_ref<const std::string&>();
    if ('$' != option_name[0]) {
      auto iter = name2p.find(option_name);
      if (name2p.end() == iter) {
        THROW_NotFound(std::string(option_class) +
                       " varname = \"" + option_name + "\"");
      }
      return iter->second;
    }
    else {
      const std::string inst_id = PluginParseInstID(option_name);
      auto iter = name2p.find(inst_id);
      if (name2p.end() == iter) {
          THROW_NotFound(std::string(option_class) +
                         " varname = \"" + inst_id + "\"");
      }
      return iter->second;
    }
  }
  if (!option_js.is_object()) {
    THROW_InvalidArgument(
        "option_js must be string or object, but is: " + option_js.dump());
  }
  return PluginFactory<Ptr>::AcquirePlugin(option_class, option_js, repo);
}
#define ROCKSDB_OBTAIN_OPT(field, option_js, repo) \
   ObtainOPT(repo.m_impl->field, field##_class, option_js, repo)

Options JS_Options(const json& js, const SidePluginRepo& repo,
                   string* name, string* path) {
  if (!js.is_object()) {
    THROW_InvalidArgument( "param json must be an object");
  }
  auto iter = js.find("name");
  if (js.end() == iter) {
    THROW_InvalidArgument("missing param \"name\"");
  }
  *name = iter.value().get_ref<const std::string&>();
  iter = js.find("path");
  if (js.end() == iter) {
      THROW_InvalidArgument("missing param \"path\", this should be a bug");
  }
  *path = iter.value().get_ref<const std::string&>();
  iter = js.find("options");
  if (js.end() == iter) {
    iter = js.find("db_options");
    if (js.end() == iter) {
      THROW_InvalidArgument("missing param \"db_options\"");
    }
    auto& db_options_js = iter.value();
    iter = js.find("cf_options");
    if (js.end() == iter) {
      THROW_InvalidArgument("missing param \"cf_options\"");
    }
    auto& cf_options_js = iter.value();
    auto db_options = ROCKSDB_OBTAIN_OPT(db_options, db_options_js, repo);
    auto cf_options = ROCKSDB_OBTAIN_OPT(cf_options, cf_options_js, repo);
    return Options(*db_options, *cf_options);
  }
  auto& js_options = iter.value();
  DBOptions_Json db_options(js_options, repo);
  ColumnFamilyOptions_Json cf_options(js_options, repo);
  return Options(db_options, cf_options);
}

static void Json_DB_Statistics(const Statistics* st, json& djs,
                               bool html, bool nozero) {
  djs["histograms"]; // insert "histograms"
  std::string name;
  if (html) {
    char buf[128];
    auto len = sprintf(buf, "name : &nbsp;"
      R"(<a href='javascript:SetParam("nozero","%d")'>nozero=%d</a>)",
      !nozero, !nozero);
    name.assign(buf, len);
  }
  else {
    name = "name";
  }
  json& tickers = djs["tickers"];
  json& histograms = djs["histograms"];
  if (!st) {
    tickers = "Statistics Is Turned Off";
    histograms = "Statistics Is Turned Off";
    return;
  }
  for (const auto& t : TickersNameMap) {
    assert(t.first < TICKER_ENUM_MAX);
    uint64_t value = st->getTickerCount(t.first);
    if (!nozero || value)
      tickers[t.second] = value;
  }
  for (const auto& h : HistogramsNameMap) {
    assert(h.first < HISTOGRAM_ENUM_MAX);
    HistogramData hData;
    st->histogramData(h.first, &hData);
    if (nozero && 0 == hData.count) {
      continue;
    }
    json cur;
    cur[name] = h.second;
    cur["P50"] = hData.median;
    cur["P95"] = hData.percentile95;
    cur["P99"] = hData.percentile99;
    cur["AVG"] = hData.average;
    cur["MIN"] = hData.min;
    cur["MAX"] = hData.max;
    cur["CNT"] = hData.count;
    cur["STD"] = hData.standard_deviation;
    cur["SUM"] = hData.sum;
    histograms.push_back(std::move(cur));
  }
  if (html) {
    histograms[0]["<htmltab:col>"] = json::array({
      name, "P50", "P95", "P99", "AVG", "MIN", "MAX", "CNT", "STD", "SUM"
    });
  }
  djs["stats_level"] = enum_stdstr(st->get_stats_level());
}

static void replace_substr(std::string& s, const std::string& f,
                           const std::string& t) {
    assert(not f.empty());
    for (auto pos = s.find(f);                // find first occurrence of f
            pos != std::string::npos;         // make sure f was found
            s.replace(pos, f.size(), t),      // replace with t, and
            pos = s.find(f, pos + t.size()))  // find next occurrence of f
    {}
}

static string& metrics_DB_Staticstics(const Statistics* st, string& res) {
  std::ostringstream oss;

  auto replace=[](const string &name) {
    string res = name;
    const string str_rocksdb {"rocksdb"};
    const string str_engine {"engine"};
    for (auto &c:res) { if (c == '.') c = ':'; }
    replace_substr(res, str_rocksdb, str_engine);

    return res;
  };

  auto sth = dynamic_cast<const StatisticsWithDiscards*>(st);
  auto rsd = std::make_unique<RawStatisticsData>();
  sth->GetAggregated(rsd->tickers, rsd->histograms);
  for (const auto& t : TickersNameMap) {
    assert(t.first < TICKER_ENUM_MAX);
    if (sth->m_discard_tickers[t.first]) continue;
    uint64_t value = rsd->tickers[t.first];
    if (value) oss|replace(t.second)|" "|value|"\n";
  }

  auto const bucket_num = bucketMapper.BucketCount();
  const auto empty = [](){};
  const string suffix_sum{"_sum"};
  const string suffix_count{"_count"};
  const string suffix_bucket{"_bucket"};
  const auto max_flag = [&]{oss|"le=\"+Inf\"";}; //bucket 最大是固定的+Inf
  for (const auto& h : HistogramsNameMap) {
    assert(h.first < HISTOGRAM_ENUM_MAX);
    if (sth->m_discard_histograms[h.first]) continue;

    auto const &buckets = rsd->histograms[h.first].buckets_;
    u_int64_t last = 0;
    size_t limit = 0;
    for (size_t i = bucketMapper.BucketCount() - 1; i > 0; i--) {
      if (buckets[i] > 0) { limit = i + 1; break; }
    }

    auto append_result=[&h,&oss,&bucket_num,&replace](const string &suffix, auto label, const uint64_t value){
      oss|replace(h.second)|suffix|"{";
      label();
      oss|"} "|value|"\n";
    };

    for (size_t i = 0; i < limit; i++) {
      auto label = [&]() {
        oss | "le=\"" | bucketMapper.BucketLimit(i) | "\"";
      };
      last += buckets[i];
      append_result(suffix_bucket, label, last);
    }

    append_result(suffix_bucket, max_flag, last);
    append_result(suffix_sum, empty, rsd->histograms[h.first].sum());
    append_result(suffix_count, empty, rsd->histograms[h.first].num());
  }

  res.append(oss.str());
  return res;
}

struct Statistics_Manip : PluginManipFunc<Statistics> {
  void Update(Statistics* db, const json& js,
              const SidePluginRepo& repo) const final {
  }
  std::string ToString(const Statistics& db, const json& dump_options,
                       const SidePluginRepo& repo) const final {
    bool html = JsonSmartBool(dump_options, "html", true);
    bool nozero = JsonSmartBool(dump_options, "nozero");
    bool metric = JsonSmartBool(dump_options, "metric");

    if (metric) {
      string res;
      metrics_DB_Staticstics(&db, res);
      return res;
    } else {
      json djs;
      Json_DB_Statistics(&db, djs, html, nozero);
      return JsonToString(djs, dump_options);
    }
  }
};
ROCKSDB_REG_PluginManip("default", Statistics_Manip);
ROCKSDB_REG_PluginManip("Default", Statistics_Manip);
ROCKSDB_REG_PluginManip("Statistics", Statistics_Manip);

static void chomp(std::string& s) {
  while (!s.empty() && isspace((unsigned char)s.back())) {
    s.pop_back();
  }
}
static std::string html_pre(const std::string& value) {
  std::string str;
  str.reserve(value.size() + 11);
  str.append("<pre>");
  str.append(value);
  str.append("</pre>");
  return str;
}
static std::string html_wrap(Slice txt) {
  std::string s;
  for (size_t i = 0; i < txt.size_; ++i) {
    const char c = txt.data_[i];
    if ('.' == c || '-' == c) {
      s.append("<br/>");
    } else {
      s.push_back(c);
    }
  }
  return s;
}
#define HTML_WRAP(txt) (html ? html_wrap(txt) : txt)

static void
GetAggregatedTableProperties(const DB& db, ColumnFamilyHandle* cfh,
                             json& djs, int level, bool html) {
  std::string propName;
  if (level < 0) {
    propName = DB::Properties::kAggregatedTableProperties;
  }
  else {
    char buf[32];
    propName.reserve(DB::Properties::kAggregatedTablePropertiesAtLevel.size() + 10);
    propName.append(DB::Properties::kAggregatedTablePropertiesAtLevel);
    propName.append(buf, snprintf(buf, sizeof buf, "%d", level));
  }
  std::string value;
  if (const_cast<DB&>(db).GetProperty(cfh, propName, &value)) {
    replace_substr(value, "; ", "\r\n");
    chomp(value);
    if (html) {
      value.reserve(value.size() + 11);
      value.insert(0, "<pre>");
      value.append("</pre>");
    }
    djs[HTML_WRAP(propName)] = std::move(value);
  }
  else {
    djs[HTML_WRAP(propName)] = "GetProperty Fail";
  }
}

void split(Slice rope, Slice delim, std::vector<std::pair<Slice, Slice> >& F) {
  F.resize(0);
  size_t dlen = delim.size();
  const char *col = rope.data(), *End = rope.data() + rope.size();
  while (col <= End) {
    auto eq = (const char*)memchr(col, '=', End-col);
    if (!eq) {
      break;
    }
    auto next = (const char*)memmem(eq+1, End-eq-1, delim.data(), dlen);
    if (next) {
      F.emplace_back(Slice(col, eq-col), Slice(eq+1, next-eq-1));
    } else {
      F.emplace_back(Slice(col, eq-col), Slice(eq+1, End-eq-1));
      break;
    }
    col = next + dlen;
  }
}

const static std::string index_block_size_name[] = {
   "index block size : user-key : delta-value", // json
   "<div style='white-space: nowrap'>index block size :<br/>user-key : delta-value</div>", // html
};

static void
GetAggregatedTablePropertiesTab(const DB& db, ColumnFamilyHandle* cfh,
                                json& djs, bool html, bool nozero) {
  std::string sum;
  auto& pjs = djs[HTML_WRAP(DB::Properties::kAggregatedTableProperties)];
  if (!const_cast<DB&>(db).GetProperty(
        cfh, DB::Properties::kAggregatedTableProperties, &sum)) {
    pjs = "GetProperty Fail";
    return;
  }
  static const std::set<Slice> ban_fields = {
      "filter policy name",
      "prefix extractor name",
      "column family ID",
      "column family name",
      "comparator name",
      "merge operator name",
      "property collectors names",
      "SST file compression options",
      "DB identity",
      "DB session identity",
  };
  auto vec_ban_fields = [](std::vector<std::pair<Slice, Slice> >& v) {
    auto is_ban = [](const std::pair<Slice, Slice>& kv) {
      return ban_fields.count(kv.first) != 0;
    };
    v.erase(std::remove_if(v.begin(), v.end(), is_ban), v.end());
  };
  pjs = json::array();
  std::vector<std::pair<Slice, Slice> > header, fields;
  split(sum, "; ", header);
  vec_ban_fields(header);
  std::string propName;
  propName.reserve(DB::Properties::kAggregatedTablePropertiesAtLevel.size() + 10);
  auto set_elem = [&](json& elem, Slice name, Slice value) {
    if (!name.starts_with("index block size")) {
        elem[name.ToString()] = value.ToString();
        return;
    }
    // sample: "index block size (user-key? 185, delta-value? 185)"
    const char* user_key_beg = name.data_ + strlen("index block size (user-key? ");
    const char* user_key_end = strchr(user_key_beg, ',');
    const char* delta_value_beg = user_key_end + strlen(", delta-value? ");
    const char* delta_value_end = strchr(delta_value_beg, ')');
    std::string result; result.reserve(32);
    result.append(value.data_, value.size_);
    result.append(" : ");
    result.append(user_key_beg, user_key_end);
    result.append(" : ");
    result.append(delta_value_beg, delta_value_end);
    elem[index_block_size_name[html?1:0]] = std::move(result);
  };
  int num_levels = const_cast<DB&>(db).NumberLevels(cfh);
  for (int level = 0; level < num_levels; level++) {
    char buf[32];
    propName.assign(DB::Properties::kAggregatedTablePropertiesAtLevel);
    propName.append(buf, snprintf(buf, sizeof buf, "%d", level));
    std::string value;
    json elem;
    elem["Level"] = level;
    if (const_cast<DB&>(db).GetProperty(cfh, propName, &value)) {
      split(value, "; ", fields);
      vec_ban_fields(fields);
      for (auto& kv : fields) {
        set_elem(elem, kv.first, kv.second);
      }
    }
    else {
      for (auto& kv : header) {
        elem[kv.first.ToString()] = "Fail";
      }
    }
    if (nozero) {
      auto iter = elem.find("# entries");
      if (elem.end() != iter &&
          iter.value().get_ref<const std::string&>() == "0") {
        continue;
      }
    }
    pjs.push_back(std::move(elem));
  }
  {
    json elem;
    elem["Level"] = "sum";
    for (auto& kv : header) {
      set_elem(elem, kv.first, kv.second);
    }
    pjs.push_back(std::move(elem));
  }
  if (html) {
    auto& fieldsNames = pjs[0]["<htmltab:col>"];
    fieldsNames = json::array();
    fieldsNames.push_back("Level");
    for (auto& kv : header) {
      if (kv.first.starts_with("index block size"))
        fieldsNames.push_back(index_block_size_name[1]);
      else
        fieldsNames.push_back(kv.first.ToString());
    }
  }
}

static size_t StrDateTime(char* buf, const char* fmt, time_t rawtime) {
  time(&rawtime);
  struct tm t; // NOLINT
  struct tm* timeinfo = localtime_r(&rawtime, &t);
  return strftime(buf, 64, fmt, timeinfo);
}

static void Html_AppendTime(std::string& html, char* buf, uint64_t t) {
  if (t)
    html.append(buf, StrDateTime(buf, "<td>%F %T</td>", t));
  else
    html.append("<td>0</td>");
}

#define AppendFmt(...) html.append(buf, snprintf(buf, sizeof(buf), __VA_ARGS__))
void Html_AppendInternalKey(std::string& html, Slice ikey,
                            const UserKeyCoder* coder) {
  char buf[32];
  ParsedInternalKey pikey;
  Status s = ParseInternalKey(ikey, &pikey, true);
  if (s.ok()) {
    html.append("<td class='monoleft'>");
    if (coder) {
      html.append(coder->Decode(pikey.user_key));
    } else {
      html.append(Slice(pikey.user_key).ToString(true));
    }
    html.append("</td>");
    html.append("<td>");
    AppendFmt("%" PRIu64, pikey.sequence);
    html.append("</td>");
    html.append("<td class='center'>");
    AppendFmt("%d", pikey.type);
    html.append("</td>");
  }
  else {
    html.append("<td colspan=3 style='color:red'>");
    html.append(s.ToString());
    html.append("<br>");
    html.append(ikey.ToString(true));
    html.append("</td>");
  }
}

std::string Json_dbname(const DB* db, const SidePluginRepo& repo) {
  auto iter = repo.m_impl->db.p2name.find(db);
  if (repo.m_impl->db.p2name.end() == iter) {
    THROW_NotFound("db.p2name.find(), db.path = " + db->GetName());
  }
  return iter->second.name;
}

std::string AggregateNames(const std::map<std::string, int>& map, const char* delim);

struct ScopeLockVersion {
  ScopeLockVersion(const ScopeLockVersion&) = delete;
  ScopeLockVersion& operator=(const ScopeLockVersion&) = delete;
  ScopeLockVersion(class ColumnFamilyData*, const DB*);
  ScopeLockVersion(class ColumnFamilyData*, class InstrumentedMutex*);
  ~ScopeLockVersion();
  class Version* version;
  class InstrumentedMutex* mutex;
};

extern InstrumentedMutex* Get_DB_mutex(const DB*);
ScopeLockVersion::ScopeLockVersion(ColumnFamilyData* cfd, const DB* db)
 : ScopeLockVersion(cfd, Get_DB_mutex(db))
{ }
ScopeLockVersion::ScopeLockVersion(ColumnFamilyData* cfd, InstrumentedMutex* mtx) {
  mtx->Lock();
  version = cfd->current();
  version->Ref();
  mtx->Unlock();
  mutex = mtx;
}
ScopeLockVersion::~ScopeLockVersion() {
  mutex->Lock();
  version->Unref();
  mutex->Unlock();
}

std::string Json_DB_CF_SST_HtmlTable(Version* version, ColumnFamilyData* cfd) {
  return Json_DB_CF_SST_HtmlTable(version, cfd, nullptr);
}
std::string Json_DB_CF_SST_HtmlTable(Version* version, ColumnFamilyData* cfd, TableProperties* all_agg) {
  std::string html;
#if defined(NDEBUG)
try {
#endif
  char buf[128];
  ColumnFamilyMetaData meta;
  TablePropertiesCollection props;
  version->GetColumnFamilyMetaData(&meta);
  {
    Status s = version->GetPropertiesOfAllTables(&props);
    if (!s.ok()) {
      html = "GetPropertiesOfAllTables() fail: " + s.ToString();
      return html;
    }
  }
  auto coder_sp = cfd->GetLatestCFOptions().html_user_key_coder;
  const UserKeyCoder* coder = nullptr;
  if (coder_sp) coder = dynamic_cast<const UserKeyCoder*>(coder_sp.get());

  auto comp = cfd->user_comparator();
  struct SstProp : SstFileMetaData, TableProperties {
    SstProp() { smallest_seqno = UINT64_MAX; }
  };
  //int max_open_files = const_cast<DB&>(db).GetDBOptions().max_open_files;
  std::vector<SstProp> levels_agg(meta.levels.size());
  std::map<std::string, int> algos_all;
  std::map<std::string, int> path_idm;
  {
    auto iopt = cfd->ioptions();
    for (int id = 0, n = (int)iopt->cf_paths.size(); id < n; ++id) {
      auto& path = iopt->cf_paths[id].path;
      path_idm[path] = id;
    }
  }
  auto agg_sst = [&](SstProp& agg, const SstFileMetaData& x,
                     const TableProperties* p, uint64_t num_compacting) {
    uint64_t file_creation_time;
    if (nullptr == p) {
      file_creation_time = x.file_creation_time;
    } else {
      agg.Add(*p);
      file_creation_time = p->file_creation_time;
    }
    if (file_creation_time && file_creation_time < agg.TableProperties::file_creation_time) {
      agg.TableProperties::file_creation_time = file_creation_time;
    }
    agg.smallest_seqno = std::min(agg.smallest_seqno, x.smallest_seqno);
    agg.largest_seqno = std::max(agg.largest_seqno, x.largest_seqno);
    if (agg.smallest_ikey.empty()) {
      agg.smallest_ikey = x.smallest_ikey;
    } else if (comp->Compare(agg.smallest_ikey, x.smallest_ikey) > 0) {
      agg.smallest_ikey = x.smallest_ikey;
    }
    if (agg.largest_ikey.empty()) {
      agg.largest_ikey = x.largest_ikey;
    } else if (comp->Compare(agg.largest_ikey, x.largest_ikey) < 0) {
      agg.largest_ikey = x.largest_ikey;
    }
    agg.size += x.size;
    agg.creation_time += num_compacting; // use creation_time as num_compacting
    agg.num_reads_sampled += x.num_reads_sampled;
  };
  for (int level = 0; level < (int)meta.levels.size(); level++) {
    auto& curr_level = meta.levels[level];
    auto& agg = levels_agg[level];
    std::map<std::string, int> algos;
    for (const auto & x : curr_level.files) {
      std::string fullname = x.db_path + x.name;
      auto iter = props.find(fullname);
      const TableProperties* p = nullptr;
      if (props.end() != iter) {
        p = iter->second.get();
        algos[p->compression_name]++;
      }
      agg_sst(agg, x, p, x.being_compacted?1:0);
    }
    for (auto& kv : algos) {
      algos_all[kv.first] += kv.second;
    }
    agg.compression_name = AggregateNames(algos, "<br>");
  }
  auto write = [&](const SstFileMetaData& x, const TableProperties* p, int fcnt) {
    if (x.being_compacted)
      html.append("<tr class='bghighlight'>");
    else
      html.append("<tr>");
    if (x.name.empty() || 'L' == x.name[0]) { // is aggregated
      html.append("<th>");
      html.append(x.name.empty() ? "sum" : x.name);
      html.append("</th>");
      AppendFmt("<th>%" PRIu64 "</th>", p->creation_time); // num_compacting
    } else { // is an sst file
      auto beg = x.name.begin();
      auto dot = std::find(beg, x.name.end(), '.');
      html.append("<th>");
      html.append("<a href='javascript:sst_file_href(");
      AppendFmt("%" PRIu64, x.file_number);
      AppendFmt(",%d", path_idm[x.db_path]);
      AppendFmt(",%zd", x.size);
      AppendFmt(",%" PRIu64, x.smallest_seqno);
      AppendFmt(",%" PRIu64, x.largest_seqno);
      html.append(")'>");
      html.append(beg + ('/' == beg[0]), dot);
      html.append("</a>");
      html.append("</th>");
      html.append("<th class='emoji'>");
      //html.append(x.being_compacted ? "true" : "false");
      //html.append(x.being_compacted ? "&#9989;" : "&#10062;"); // yes/no
      //html.append(x.being_compacted ? "&#128308;" : "&#128309;"); // red/blue circle
      html.append(x.being_compacted ? "&#128994;" : "&#128309;"); // green/blue circle
      html.append("</th>");
    }
    AppendFmt("<td>%.6f</td>", x.size/1e9);
    uint64_t file_creation_time;
    if (!p) {
      html.append("<td>unkown</td>"); // raw size (key + value)
      AppendFmt("%" PRIu64, x.num_entries);
      AppendFmt("%" PRIu64, x.num_deletions);
      html.append("<td>&#10067;</td>"); // merge
      html.append("<td>&#10067;</td>"); // range del
      html.append("<td>&#10067;</td>"); // data blocks
      html.append("<td>&#10067;</td>"); // sum zip key = index size
      html.append("<td>&#10067;</td>"); // sum zip val = data size
      html.append("<td>&#10067;</td>"); // sum raw key size
      html.append("<td>&#10067;</td>"); // sum raw val size
      html.append("<td>&#10067;</td>"); // key_zip_ratio
      html.append("<td>&#10067;</td>"); // val_zip_ratio
      html.append("<td>&#10067;</td>"); // kv_zip_ratio
      html.append("<td>&#10067;</td>"); // avg zip key size
      html.append("<td>&#10067;</td>"); // avg raw key size
      html.append("<td>&#10067;</td>"); // avg zip val size
      html.append("<td>&#10067;</td>"); // avg raw val size
      html.append("<td>&#10067;</td>"); // compression name
      //html.append("<td>&#10067;</td>"); // oldest_key_time
      file_creation_time = x.file_creation_time;
    } else {
      auto avg_raw_key = double(p->raw_key_size) / p->num_entries;
      auto avg_raw_val = double(p->raw_value_size) / p->num_entries;
      auto avg_zip_key = double(p->index_size) / p->num_entries;
      auto avg_zip_val = double(p->data_size) / p->num_entries;
      auto kv_zip_size = double(p->index_size + p->data_size);
      auto kv_raw_size = double(p->raw_key_size + p->raw_value_size);
      auto key_zip_ratio = double(p->index_size)/p->raw_key_size;
      auto val_zip_ratio = double(p->data_size)/p->raw_value_size;
      auto kv_zip_ratio = kv_zip_size/kv_raw_size;
      AppendFmt("<td>%.6f</td>", kv_raw_size/1e9);
      AppendFmt("<td>%" PRIu64"</td>", p->num_entries);
      AppendFmt("<td>%" PRIu64"</td>", p->num_deletions);
      AppendFmt("<td>%" PRIu64"</td>", p->num_merge_operands);
      AppendFmt("<td>%" PRIu64"</td>", p->num_range_deletions);
      AppendFmt("<td>%" PRIu64"</td>", p->num_data_blocks);
      AppendFmt("<td>%.6f</td>", p->index_size/1e9);
      AppendFmt("<td>%.6f</td>", p->data_size/1e9);
      AppendFmt("<td>%.3f</td>", p->index_size/kv_zip_size);
      AppendFmt("<td>%.6f</td>", p->raw_key_size/1e9);
      AppendFmt("<td>%.6f</td>", p->raw_value_size/1e9);
      AppendFmt("<td>%.3f</td>", p->raw_key_size/kv_raw_size);
      AppendFmt("<td>%.3f</td>", key_zip_ratio);
      AppendFmt("<td>%.3f</td>", val_zip_ratio);
      AppendFmt("<td>%.3f</td>", kv_zip_ratio);
      AppendFmt("<td>%.3f</td>", avg_zip_key);
      AppendFmt("<td>%.1f</td>", avg_raw_key);
      AppendFmt("<td>%.3f</td>", avg_zip_val);
      AppendFmt("<td>%.1f</td>", avg_raw_val);
      html.append("<td class='left'>");
      html.append(p->compression_name);
      html.append("</td>");
      //AppendFmt("<td>%" PRIu64 "</td>", p->oldest_key_time);
      file_creation_time = p->file_creation_time;
    }
    AppendFmt("<td>%" PRIu64"</td>", x.smallest_seqno);
    AppendFmt("<td>%" PRIu64"</td>", x.largest_seqno);
    Html_AppendInternalKey(html, x.smallest_ikey, coder);
    Html_AppendInternalKey(html, x.largest_ikey, coder);
    Html_AppendTime(html, buf, file_creation_time);
    AppendFmt("<td>%" PRIu64"</td>", x.num_reads_sampled);
    if (fcnt >= 0) {
      AppendFmt("<td>%d</td>", fcnt);
    }
    html.append("</tr>\n");
  };

  auto writeHeader = [&](bool with_fcnt) {
    html.append("<thead><tr>");
    html.append("<th rowspan=2>Name</th>");
    //html.append("<th rowspan=2>&#127959;</th>"); // compacting: 施工中
    //html.append("<th rowspan=2>&#127540;</th>"); // compacting: character: "合"
    //compacting: emoji: recycling
    html.append("<th rowspan=2 class='emoji' style='font-size: xx-large' title='being compacted ?'>&#9851;</th>");
    html.append("<th colspan=2>FileSize(GB)</th>");
    html.append("<th colspan=4>Entries</th>");
    html.append("<th rowspan=2 title='Data Blocks'>B</th>");
    html.append("<th colspan=3>ZipSize(GB)</th>");
    html.append("<th colspan=3>RawSize(GB)</th>");
    html.append("<th colspan=3>ZipRatio(Zip/Raw)</th>");
    html.append("<th colspan=2>AvgKey</th>");
    html.append("<th colspan=2>AvgValue</th>");
    html.append("<th rowspan=2>ZipAlgo</th>");
    html.append("<th rowspan=2>Smallest<br>SeqNum</th>");
    html.append("<th rowspan=2>Largest<br>SeqNum</th>");
    html.append("<th colspan=3>Smallest Key</th>");
    html.append("<th colspan=3>Largest Key</th>");
    html.append("<th rowspan=2>FileTime</th>");
    html.append("<th rowspan=2>NumReads<br>Sampled</th>");
    if (with_fcnt) {
      html.append("<th rowspan=2>File<br>CNT</th>");
    }
    html.append("</tr>");
    html.append("<tr>");
    html.append("<th>Zip</th>");
    html.append("<th>Raw</th>");

    html.append("<th title='All Entries'>All</th>");
    html.append("<th title='Deletions'>D</th>");
    html.append("<th title='Merges'>M</th>");
    html.append("<th title='Range Deletions'>R</th>");

    html.append("<th>Key</th>");
    html.append("<th>Value</th>");
    html.append("<th title='size ratio of Key/(Key + Value)'>K/KV</th>");
    html.append("<th>Key</th>");
    html.append("<th>Value</th>");
    html.append("<th title='size ratio of Key/(Key + Value)'>K/KV</th>");

    html.append("<th>Key</th>");     // Zip Ratio
    html.append("<th>Value</th>");
    html.append("<th>K+V</th>");

    html.append("<th>Zip</th>");  // AvgLen
    html.append("<th>Raw</th>");
    html.append("<th>Zip</th>");
    html.append("<th>Raw</th>");

    html.append("<th>UserKey</th>");
    html.append("<th>Seq</th>");
    html.append("<th>Type</th>");
    html.append("<th>UserKey</th>");
    html.append("<th>Seq</th>");
    html.append("<th>Type</th>");
    html.append("</tr></thead>");
  };
  html.append("<div>");
  html.append(R"EOS(
  <script>
    function sst_file_href(file, path_id, fsize, smallest, largest) {
      location.href = location.href
         + "&file=" + file
         + "&path=" + path_id
         + "&size=" + fsize
         + "&smallest=" + smallest
         + "&largest=" + largest
         ;
    }
  </script>
  <style>
    div * td {
      text-align: right;
      font-family: monospace;
    }
    div * td.left {
      text-align: left;
      font-family: Sans-serif;
    }
    div * td.monoleft {
      text-align: left;
      font-family: monospace;
    }
    div * td.center {
      text-align: center;
      font-family: Sans-serif;
    }
    .bghighlight {
      background-color: AntiqueWhite;
    }
    .emoji {
      font-family: "Apple Color Emoji","Segoe UI Emoji",NotoColorEmoji,"Segoe UI Symbol","Android Emoji",EmojiSymbols;
    }
    em {
      color: brown;
    }
  </style>)EOS"
  );
  html.append("<p>");
  AppendFmt("all levels summary: file count = %zd, ", meta.file_count);
  AppendFmt("total size = %.3f GB", meta.size/1e9);
  html.append("</p>\n");
  html.append("<table border=1>\n");
  writeHeader(true);
  html.append("<tbody>\n");
  SstProp all_levels_agg;
  for (int level = 0; level < (int)levels_agg.size(); level++) {
    auto& curr_agg = levels_agg[level];
    if (0 == curr_agg.size) {
      continue;
    }
    curr_agg.name.assign(buf, snprintf(buf, sizeof buf, "L%d", level));
    write(curr_agg, &curr_agg, (int)meta.levels[level].files.size());
    // use creation_time as num_compacting
    agg_sst(all_levels_agg, curr_agg, &curr_agg, curr_agg.creation_time);
  }
  all_levels_agg.compression_name = AggregateNames(algos_all, "<br>");
  if (all_agg) {
    *all_agg = all_levels_agg;
  }
  html.append("</tbody>\n");
  html.append("<tfoot>\n");
  write(all_levels_agg, &all_levels_agg, (int)meta.file_count);
  html.append("</tfoot></table>\n");

  for (int level = 0; level < (int)meta.levels.size(); level++) {
    auto& curr_level = meta.levels[level];
    if (curr_level.files.empty()) {
      continue;
    }
    html.append("<hr><p>");
    AppendFmt("level = %d, ", curr_level.level);
    AppendFmt("file count = %zd, ", curr_level.files.size());
    AppendFmt("total size = %.3f GB", curr_level.size/1e9);
    html.append("</p>\n");
    html.append("<table border=1>\n");
    writeHeader(false);
    html.append("<tbody>\n");
    for (const auto & x : curr_level.files) {
      std::string fullname = x.db_path + x.name;
      html.append("<tr>");
      auto iter = props.find(fullname);
      if (props.end() == iter) {
        write(x, nullptr, -1);
      } else {
        write(x, iter->second.get(), -1);
      }
    }
    html.append("</tbody>\n");
    html.append("<tfoot>\n");
    levels_agg[level].name = ""; // sum
    write(levels_agg[level], &levels_agg[level], -1);
    html.append("</tfoot></table>\n");
  }
  html.append("</div>");
  return html;
#if defined(NDEBUG)
}
catch (const std::exception& ex) {
  throw std::runtime_error(html + "\n" + ex.what());
}
#endif
}

Slice SliceSlice(Slice big, Slice sub) {
  auto pos = (const char*)memmem(big.data_, big.size_, sub.data_, sub.size_);
  return Slice(pos, sub.size_);
}

static std::string
Json_DB_NoFileHistogram_Add_convenient_links(
        const std::string& db,
        const std::string& cf,
        const std::string& str) {
  Slice big = str;
  Slice pos = SliceSlice(big, "\nUptime(secs):");
  if (pos.data_) {
    std::ostringstream oss;
    oss << "<pre>";
    oss.write(big.data_, pos.data_ - big.data_);
    auto write_space = [&](size_t lo, size_t hi) {
      for (; lo < hi; ++lo) oss.write(" ", 1);
    };
    size_t left_width = 50;
    write_space(0, left_width);
    oss|"<a href='/props/"|db|"/"|cf|"?html=1'>noint=0</a>  ";
    oss|"<a href='/props/"|db|"/"|cf|"?html=1&sst=1'>noint=0&amp;sst=1</a>  ";
    oss|"<a href='/props/"|db|"/"|cf|"?html=1&sst=2'>noint=0&amp;sst=2</a>  ";

    oss|"<a href='/props/"|db|"/"|cf|"?html=1&nozero=1'>noint=0&amp;nozero=1</a>  ";
    oss|"<a href='/props/"|db|"/"|cf|"?html=1&nozero=1&sst=1'>noint=0&amp;nozero=1&amp;sst=1</a>  ";
    oss|"<a href='/props/"|db|"/"|cf|"?html=1&nozero=1&sst=2'>noint=0&amp;nozero=1&amp;sst=2</a>";

    auto lf = (const char*)memchr(pos.end(), '\n', big.data_ - pos.end());
    if (lf) {
      size_t len = lf - pos.data_;
      oss.write(pos.data_, len);
      write_space(len-1, left_width); // len-1 to exclude the starting '\n'
      pos = Slice(lf, big.end() - lf);
    }
    else { // insert a line, should not happen, just for fallback
      oss << "\n";
      write_space(0, left_width);
      pos = Slice(pos.end(), big.end() - pos.end());
    }

    oss|"<a href='/props/"|db|"/"|cf|"?html=1&noint=1'>noint=1</a>  ";
    oss|"<a href='/props/"|db|"/"|cf|"?html=1&noint=1&sst=1'>noint=1&amp;sst=1</a>  ";
    oss|"<a href='/props/"|db|"/"|cf|"?html=1&noint=1&sst=2'>noint=1&amp;sst=2</a>  ";

    oss|"<a href='/props/"|db|"/"|cf|"?html=1&noint=1&nozero=1'>noint=1&amp;nozero=1</a>  ";
    oss|"<a href='/props/"|db|"/"|cf|"?html=1&noint=1&nozero=1&sst=1'>noint=1&amp;nozero=1&amp;sst=1</a>  ";
    oss|"<a href='/props/"|db|"/"|cf|"?html=1&noint=1&nozero=1&sst=2'>noint=1&amp;nozero=1&amp;sst=2</a>";

    oss.write(pos.data_, pos.size_);
    oss << "</pre>";
    return oss.str();
  }
  else {
    return html_pre(str);
  }
}
static void
Json_DB_Level_Stats(const DB& db, ColumnFamilyHandle* cfh, json& djs,
                    bool html, const json& dump_options,
                    const SidePluginRepo& repo) {
  static const std::string* aStrProps[] = {
    //&DB::Properties::kNumFilesAtLevelPrefix,
    //&DB::Properties::kCompressionRatioAtLevelPrefix,
    //&DB::Properties::kStats,
    //&DB::Properties::kSSTables,
    //&DB::Properties::kCFStats,
    &DB::Properties::kCFStatsNoFileHistogram,
    &DB::Properties::kCFFileHistogram,
    &DB::Properties::kDBStats,
    &DB::Properties::kLevelStats,
    //&DB::Properties::kAggregatedTableProperties,
    //&DB::Properties::kAggregatedTablePropertiesAtLevel,
  };
  // if noint, promote StrProps to top json
  auto& stjs = JsonSmartBool(dump_options, "noint") ? djs : djs["StrProps"];
  auto prop_to_js = [&](const std::string& name) {
    std::string value;
    if (const_cast<DB&>(db).GetProperty(cfh, name, &value)) {
      if (html) {
        if (name == DB::Properties::kCFStatsNoFileHistogram) {
          const std::string dbname = basename(db.GetName().c_str());
          const std::string cfname = cfh->GetName();
          stjs[HTML_WRAP(name)] = Json_DB_NoFileHistogram_Add_convenient_links(dbname, cfname, value);
        }
        else {
          stjs[HTML_WRAP(name)] = html_pre(value);
        }
      }
      else {
        stjs[HTML_WRAP(name)] = std::move(value);
      }
    } else {
      stjs[HTML_WRAP(name)] = "GetProperty Fail";
    }
  };
  for (auto pName : aStrProps) {
    prop_to_js(*pName);
  }
  int arg_sst = JsonSmartInt(dump_options, "sst", 0);
  if (!html && arg_sst) {
      prop_to_js(DB::Properties::kSSTables);
  }
  else switch (arg_sst) {
    default:
      stjs[HTML_WRAP(DB::Properties::kSSTables)] = "bad param 'sst'";
      break;
    case 0:
      break;
    case 1:
      prop_to_js(DB::Properties::kSSTables);
      break;
    case 2: // show as html table
    {
      auto cfd = cfh->cfd();
      ScopeLockVersion slv(cfd, Get_DB_mutex(&db));
      stjs[HTML_WRAP(DB::Properties::kSSTables)] = Json_DB_CF_SST_HtmlTable(slv.version, cfd);
      break;
    }
  }
  /// atp is short for Aggregated-Table-Properties
  int atp = JsonSmartInt(dump_options, "atp", 1);
  //GetAggregatedTableProperties(db, cfh, stjs, -1, html);
  // -1 is for kAggregatedTableProperties
  if (atp >= 3) {
    const int num_levels = const_cast<DB&>(db).NumberLevels(cfh);
    for (int level = -1; level < num_levels; level++) {
      GetAggregatedTableProperties(db, cfh, stjs, level, html);
    }
  } else if (atp >= 2) {
    bool nozero = JsonSmartBool(dump_options, "nozero");
    GetAggregatedTablePropertiesTab(db, cfh, stjs, html, nozero);
  }
}

static void Json_DB_IntProps(const DB& db, ColumnFamilyHandle* cfh,
                             json& djs, bool showbad, bool nozero) {
  static const std::string* aIntProps[] = {
    &DB::Properties::kNumImmutableMemTable,
    &DB::Properties::kNumImmutableMemTableFlushed,
    &DB::Properties::kMemTableFlushPending,
    &DB::Properties::kNumRunningFlushes,
    &DB::Properties::kCompactionPending,
    &DB::Properties::kNumRunningCompactions,
    &DB::Properties::kBackgroundErrors,
    &DB::Properties::kCurSizeActiveMemTable,
    &DB::Properties::kCurSizeAllMemTables,
    &DB::Properties::kSizeAllMemTables,
    &DB::Properties::kNumEntriesActiveMemTable,
    &DB::Properties::kNumEntriesImmMemTables,
    &DB::Properties::kNumDeletesActiveMemTable,
    &DB::Properties::kNumDeletesImmMemTables,
    &DB::Properties::kEstimateNumKeys,
    &DB::Properties::kEstimateTableReadersMem,
    &DB::Properties::kIsFileDeletionsEnabled,
    &DB::Properties::kNumSnapshots,
    &DB::Properties::kOldestSnapshotTime,
    &DB::Properties::kOldestSnapshotSequence,
    &DB::Properties::kNumLiveVersions,
    &DB::Properties::kCurrentSuperVersionNumber,
    &DB::Properties::kEstimateLiveDataSize,
    &DB::Properties::kMinLogNumberToKeep,
    &DB::Properties::kMinObsoleteSstNumberToKeep,
    &DB::Properties::kTotalSstFilesSize,
    &DB::Properties::kLiveSstFilesSize,
    &DB::Properties::kBaseLevel,
    &DB::Properties::kEstimatePendingCompactionBytes,
    //&DB::Properties::kAggregatedTableProperties, // string
    //&DB::Properties::kAggregatedTablePropertiesAtLevel, // string
    &DB::Properties::kActualDelayedWriteRate,
    &DB::Properties::kIsWriteStopped,
    &DB::Properties::kEstimateOldestKeyTime,
    &DB::Properties::kBlockCacheCapacity,
    &DB::Properties::kBlockCacheUsage,
    &DB::Properties::kBlockCachePinnedUsage,
  };
  auto& ipjs = djs["IntProps"];
  for (auto pName : aIntProps) {
    uint64_t value = 0;
    if (const_cast<DB&>(db).GetIntProperty(cfh, *pName, &value)) {
      if (!nozero || value)
        ipjs[*pName] = value;
    } else if (showbad) {
      ipjs[*pName] = "GetProperty Fail";
    }
  }
}

static std::string Json_DB_OneSST(const DB& db, ColumnFamilyHandle* cfh,
                                  const json& dump_options, int file_num) {
  auto cfd = cfh->cfd();
  auto tc = cfd->table_cache();
//auto mut_cfo = cfd->GetCurrentMutableCFOptions(); // must in DB mutex
  auto mut_cfo = cfd->GetLatestMutableCFOptions(); // safe if not in DB mutex
  // GetLatestMutableCFOptions: rocksdb says it is not safe if not in DB mutex
  // but it is realy safe, because the '*mut_cfo' object is a field of cfd,
  // we just use mut_cfo->prefix_extractor.get() which is an atomic load
  FileDescriptor fd(file_num,
                    JsonSmartInt(dump_options, "path", 0),
                    JsonSmartInt64(dump_options, "size", 0),
                    JsonSmartInt64(dump_options, "smallest", 0),
                    JsonSmartInt64(dump_options, "largest", kMaxSequenceNumber));
  Cache::Handle* ch = nullptr;
  auto& icmp = cfd->internal_comparator();
  auto& fopt = *cfd->soptions(); // file_options
  auto pref_ext = mut_cfo->prefix_extractor.get();
  auto s = tc->FindTable(ReadOptions(), fopt, icmp, fd, &ch, pref_ext);
  if (!s.ok()) {
    THROW_InvalidArgument(s.ToString());
  }
  ROCKSDB_VERIFY(nullptr != ch);
  ROCKSDB_SCOPE_EXIT(tc->ReleaseHandle(ch));
  TableReader* tr = tc->GetTableReaderFromHandle(ch);
  const auto& zip_algo = tr->GetTableProperties()->compression_name;
  auto manip = PluginManip<TableReader>::AcquirePlugin(zip_algo, json(), null_repo_ref());
  json dump_opt2 = dump_options;
  dump_opt2["__ptr_cfd__"] = size_t(cfd);
  return manip->ToString(*tr, dump_opt2, null_repo_ref());
}

// format not suitable for prometheus
//&DB::Properties::kLevelStats,   //multi-line string
//&DB::Properties::kStats,
//&DB::Properties::kCFStatsNoFileHistogram,
//&DB::Properties::kCFFileHistogram,
//&DB::Properties::kDBStats,
//&DB::Properties::kOptionsStatistics,
//&DB::Properties::kSSTables,

static string CFPropertiesMetric(const DB& db, ColumnFamilyHandle* cfh) {
  static const string* int_properties[] = {
    &DB::Properties::kNumImmutableMemTable,
    &DB::Properties::kNumImmutableMemTableFlushed,
    &DB::Properties::kMemTableFlushPending,
    &DB::Properties::kCompactionPending,
    &DB::Properties::kBackgroundErrors,
    &DB::Properties::kCurSizeActiveMemTable,
    &DB::Properties::kCurSizeAllMemTables,
    &DB::Properties::kSizeAllMemTables,
    &DB::Properties::kNumEntriesActiveMemTable,
    &DB::Properties::kNumEntriesImmMemTables,
    &DB::Properties::kNumDeletesActiveMemTable,
    &DB::Properties::kNumDeletesImmMemTables,
    &DB::Properties::kEstimateNumKeys,
    &DB::Properties::kEstimateTableReadersMem,
    &DB::Properties::kIsFileDeletionsEnabled,
    &DB::Properties::kNumSnapshots,
    &DB::Properties::kOldestSnapshotTime,
    &DB::Properties::kOldestSnapshotSequence,
    &DB::Properties::kNumLiveVersions,
    &DB::Properties::kCurrentSuperVersionNumber,
    &DB::Properties::kEstimateLiveDataSize,
    &DB::Properties::kMinLogNumberToKeep,
    &DB::Properties::kMinObsoleteSstNumberToKeep,
    &DB::Properties::kBaseLevel,
    &DB::Properties::kTotalSstFilesSize,
    &DB::Properties::kLiveSstFilesSize,
    &DB::Properties::kEstimatePendingCompactionBytes,
    &DB::Properties::kNumRunningFlushes,
    &DB::Properties::kNumRunningCompactions,
    &DB::Properties::kActualDelayedWriteRate,
    &DB::Properties::kIsWriteStopped,
    &DB::Properties::kEstimateOldestKeyTime,
    &DB::Properties::kBlockCacheCapacity,
    &DB::Properties::kBlockCacheUsage,
    &DB::Properties::kBlockCachePinnedUsage,
  };
  static const string* prefix_properties[] = {
    &DB::Properties::kNumFilesAtLevelPrefix,
    &DB::Properties::kCompressionRatioAtLevelPrefix,
  };
  static const string* map_properties[] = {
    &DB::Properties::kCFStats,
    &DB::Properties::kCFFileHistogram,
    &DB::Properties::kBlockCacheEntryStats,
    &DB::Properties::kAggregatedTableProperties,
  };

  std::ostringstream oss;

  const string str_rocksdb{"rocksdb"}, str_engine{"engine"};
  auto replace_char=[&str_rocksdb,&str_engine](string &name) { //adapter prmehtues key name
    for (auto &c:name) { if (c == '.') c = ':'; }
    for (auto &c:name) { if (c == '-') c = '_'; }
    replace_substr(name, str_rocksdb, str_engine);
  };

  auto add_int_properties = [&oss,&replace_char,&db,&cfh](const string* key){
    uint64_t value = 0;
    if (const_cast<DB&>(db).GetIntProperty(cfh, *key, &value)) {
      string name = *key;
      replace_char(name);
      oss|name|" "|value|"\n";
    }
  };
  for (auto const key:int_properties) { add_int_properties(key); }

  auto add_map_properties = [&oss,&replace_char,&db,&cfh](const string* key) {
    std::map<std::string, std::string> value;
    if (const_cast<DB&>(db).GetMapProperty(cfh, *key, &value)) {
      string name = *key;
      replace_char(name);
      for (auto const v_iter:value) {
        string suffix = v_iter.first;
        replace_char(suffix);
        oss|name|"{flag=\""|suffix|"\"} "|v_iter.second|"\n";
      }
    }
  };
  for(auto const key:map_properties) { add_map_properties(key); }

  auto add_prefix_properties=[&db,&cfh,&oss,&replace_char](const string *prefix) {
    const int num_levels = const_cast<DB&>(db).NumberLevels(cfh);
    for (int level = 0; level < num_levels; level++) {
      string value;
      string name = *prefix;
      name.append(std::to_string(level));
      if (const_cast<DB&>(db).GetProperty(cfh, name, &value)) {
        replace_char(name);
        oss|name|" "|value|"\n";
      }
    }
  };
  for (auto const key:prefix_properties) { add_prefix_properties(key); }

  auto add_prefix_map_properties=[&db,&cfh,&oss,&replace_char, add_map_properties](const string *prefix) {
    const int num_levels = const_cast<DB&>(db).NumberLevels(cfh);
    for (int level = 0; level < num_levels; level++) {
      string name = *prefix;
      name.append(std::to_string(level));
      add_map_properties(&name);
    }
  };
  add_prefix_map_properties(&DB::Properties::kAggregatedTablePropertiesAtLevel);

  return oss.str();
}

struct CFPropertiesWebView_Manip : PluginManipFunc<CFPropertiesWebView> {
  void Update(CFPropertiesWebView* cfp, const json& js,
              const SidePluginRepo& repo) const final {
  }
  std::string ToString(const CFPropertiesWebView& cfp, const json& dump_options,
                       const SidePluginRepo& repo) const final {
    bool html = JsonSmartBool(dump_options, "html", true);
    bool metric = JsonSmartBool(dump_options, "metric", false);
    if (metric) html = false;
    json djs;
    int file_num = JsonSmartInt(dump_options, "file", -1);
    if (file_num >= 0) {
      return Json_DB_OneSST(*cfp.db, cfp.cfh, dump_options, file_num);
    }
    if (!JsonSmartBool(dump_options, "noint")) {
      bool showbad = JsonSmartBool(dump_options, "showbad");
      bool nozero = JsonSmartBool(dump_options, "nozero");
      Json_DB_IntProps(*cfp.db, cfp.cfh, djs, showbad, nozero);
    }

    Json_DB_Level_Stats(*cfp.db, cfp.cfh, djs, html, dump_options, repo);

    if (metric) {
      return CFPropertiesMetric(*cfp.db, cfp.cfh);
    } else {
      return JsonToString(djs, dump_options);
    }
  }
};
ROCKSDB_REG_PluginManip("builtin", CFPropertiesWebView_Manip);

static void SetCFPropertiesWebView(DB* db, ColumnFamilyHandle* cfh,
                                  const std::string& dbname,
                                  const std::string& cfname,
                                  const SidePluginRepo& crepo) {
  auto& repo = const_cast<SidePluginRepo&>(crepo);
  std::string varname = dbname + "/" + cfname;
  auto view = std::make_shared<CFPropertiesWebView>(
                               CFPropertiesWebView{db, cfh});
  repo.m_impl->props.p2name.emplace(
      view.get(), SidePluginRepo::Impl::ObjInfo{varname,
      json{{"class", "builtin"}, {"params", "empty"}}});
  repo.m_impl->props.name2p->emplace(varname, view);
}
static void SetCFPropertiesWebView(DB* db, const std::string& dbname,
                                   const SidePluginRepo& crepo) {
  auto cfh = db->DefaultColumnFamily();
  SetCFPropertiesWebView(db, cfh, dbname, "default", crepo);
}
static void SetCFPropertiesWebView(DB_MultiCF* mcf, const std::string& dbname,
                                   const std::vector<ColumnFamilyDescriptor>& cfdvec,
                                   const SidePluginRepo& crepo) {
  assert(mcf->cf_handles.size() == cfdvec.size());
  DB* db = mcf->db;
  for (size_t i = 0; i < mcf->cf_handles.size(); i++) {
    auto cfh = mcf->cf_handles[i];
    const std::string& cfname = cfdvec[i].name;
    SetCFPropertiesWebView(db, cfh, dbname, cfname, crepo);
  }
}

static void
JS_Add_CFPropertiesWebView_Link(json& djs, const DB& db, bool html,
                                const SidePluginRepo& repo) {
  auto i1 = repo.m_impl->db.p2name.find(&db);
  ROCKSDB_VERIFY(repo.m_impl->db.p2name.end() != i1);
  const std::string& db_varname = i1->second.name;
  auto iter = repo.m_impl->props.name2p->find(db_varname + "/default");
  assert(repo.m_impl->props.name2p->end() != iter);
  ROCKSDB_VERIFY(repo.m_impl->props.name2p->end() != iter);
  auto properties = iter->second;
  ROCKSDB_JSON_SET_FACX(djs, properties, props);
}
static void
JS_Add_CFPropertiesWebView_Link(json& djs, bool html,
                                const std::string& dbname,
                                const std::string& cfname,
                                const SidePluginRepo& repo) {
  auto iter = repo.m_impl->props.name2p->find(dbname + "/" + cfname);
  assert(repo.m_impl->props.name2p->end() != iter);
  ROCKSDB_VERIFY(repo.m_impl->props.name2p->end() != iter);
  auto properties = iter->second;
  ROCKSDB_JSON_SET_FACX(djs, properties, props);
}
#define GITHUB_ROCKSDB "https://github.com/rockeet/rocksdb-private"
void JS_RokcsDB_AddVersion(json& djs, bool html) {
  djs["build_type"] = ROCKSDB_IF_DEBUG("debug", "release");
  auto p_sha = strchr(rocksdb_build_git_sha, ':');
  auto p_tag = strchr(rocksdb_build_git_tag, ':');
  auto p_date = strchr(rocksdb_build_date, ':');
  ROCKSDB_VERIFY(nullptr != p_sha);
  ROCKSDB_VERIFY(nullptr != p_tag);
  ROCKSDB_VERIFY(nullptr != p_date);
  p_sha += 1;
  p_tag += 1;
  p_date += 1;
  if (html) {
    std::ostringstream sha, tag;
    sha|"<a href='"|GITHUB_ROCKSDB|"/commit/"|p_sha|"'>"|p_sha|"</a>";
    tag|"<a href='"|GITHUB_ROCKSDB|"/tree/"  |p_tag|"'>"|p_tag|"</a>";
    djs["git_sha"] = sha.str();
    djs["git_tag"] = tag.str();
  } else {
    djs["git_sha"] = p_sha;
    djs["git_tag"] = p_tag;
  }
  djs["git_date"] = strchr(rocksdb_build_git_date, ':') + 1;
  djs["compile_date"] = strchr(rocksdb_build_date, ':') + 1;
  char s[32];
  auto n = snprintf(s, sizeof(s), "%d.%d.%d",
                    ROCKSDB_MAJOR, ROCKSDB_MINOR, ROCKSDB_PATCH);
  djs["version"] = std::string(s, n);
}

struct ManCompactStatus {
  time_t start_time_us;
  DB*    db;
};
static std::mutex g_running_manual_compact_mtx;
static std::map<ColumnFamilyHandle*, ManCompactStatus> g_running_manual_compact;
static std::string RunManualCompact(const DB* dbc, ColumnFamilyHandle* cfh,
                                    const json& dump_options) {
  DB* db = const_cast<DB*>(dbc);
  DBOptions dbo = db->GetDBOptions();
  ManCompactStatus mcs{::time(NULL), db};
  auto refresh = JsonSmartInt(dump_options, "refresh", 0);
  bool existed = false;
  g_running_manual_compact_mtx.lock();
  if (refresh) {
    auto iter = g_running_manual_compact.find(cfh);
    if (g_running_manual_compact.end() != iter) {
      existed = true;
      mcs = iter->second;
    }
  }
  else {
    auto ib = g_running_manual_compact.emplace(cfh, mcs);
    mcs = ib.first->second;
    existed = !ib.second;
  }
  g_running_manual_compact_mtx.unlock();
  extern int Get_DB_next_job_id(const DB* db);
  if (existed) {
    extern std::string cur_time_stat(time_t start_time, const char* up);
    json js;
    js["status"] = "running";
    js["next_job_id"] = Get_DB_next_job_id(db);
    js["time"] = cur_time_stat(mcs.start_time_us, "Elapsed");
    return JsonToString(js, dump_options);
  }
  if (refresh) {
    json js;
    js["status"] = "reject";
    js["next_job_id"] = Get_DB_next_job_id(db);
    js["description"] = "reject because refresh = " + std::to_string(refresh);
    return JsonToString(js, dump_options);
  }
  struct MyCRO : CompactRangeOptions {
    explicit MyCRO(const json& js, int target_path_id) {
      exclusive_manual_compaction = false; // change default to false
      #if 1
        #define MyCRO_GET(func, field) func(&field, js, #field)
      #else
        #define MyCRO_GET(func, field) ROCKSDB_JSON_OPT_PROP(js, field)
      #endif
      int max_subcompactions = this->max_subcompactions;
      MyCRO_GET(JsonSmartBool, exclusive_manual_compaction);
      MyCRO_GET(JsonSmartBool, change_level);
      MyCRO_GET(JsonSmartInt , target_level);
      MyCRO_GET(JsonSmartBool, allow_write_stall);
      MyCRO_GET(JsonSmartInt , max_subcompactions);
      MyCRO_GET(JsonSmartInt , target_path_id);
      this->max_subcompactions = max_subcompactions;
      this->target_path_id = target_path_id;
      ROCKSDB_JSON_OPT_ENUM(js, bottommost_level_compaction);
    }
  };
  auto& cf_paths = cfh->cfd()->ioptions()->cf_paths;
  int default_target_path_id = (int)(cf_paths.size() - 1);
  MyCRO cro(dump_options, default_target_path_id);
  std::thread([=]() {
    db->CompactRange(cro, cfh, nullptr, nullptr);
    g_running_manual_compact_mtx.lock();
    g_running_manual_compact.erase(cfh);
    g_running_manual_compact_mtx.unlock();
  }).detach();
  const std::string& dbname = db->GetName();
  const std::string& cfname = cfh->GetName();
  return "compact job issued: dbname = " + dbname + ", cfname = " + cfname;
}

struct DB_Manip : PluginManipFunc<DB> {
  void Update(DB* db, const json& js,
              const SidePluginRepo& repo) const final {
  }
  std::string ToString(const DB& db, const json& dump_options,
                       const SidePluginRepo& repo) const final {
    Options opt = db.GetOptions();
    auto& dbo = static_cast<DBOptions_Json&>(static_cast<DBOptions&>(opt));
    auto& cfo = static_cast<CFOptions_Json&>(static_cast<CFOptions&>(opt));
    const auto& dbmap = repo.m_impl->db;
    json djs;
    std::string dbo_name, cfo_name;
    //const std::string& dbname = db.GetName();
    std::string dbname = Json_dbname(&db, repo);
    auto i1 = dbmap.p2name.find((DB*)&db);
    if (dbmap.p2name.end() == i1) {
      THROW_NotFound("db ptr is not registered in repo, dbname = " + dbname);
    }
    if (dump_options.contains("compact")) {
      if (repo.m_impl->web_compact)
        return RunManualCompact(&db, db.DefaultColumnFamily(), dump_options);
      else
        return "web_compact is not allowed";
    }
    auto ijs = i1->second.params.find("params");
    if (i1->second.params.end() == ijs) {
      THROW_Corruption("p2name[" + dbname + "].params is missing");
    }
    const json& params_js = ijs.value();
    if (params_js.end() != (ijs = params_js.find("options"))) {
      dbo_name = "json varname: " + ijs.value().get_ref<const std::string&>() + "(Options)";
      cfo_name = "json varname: " + ijs.value().get_ref<const std::string&>() + "(Options)";
    } else {
      if (params_js.end() != (ijs = params_js.find("db_options"))) {
        if (ijs.value().is_string())
          dbo_name = "json varname: " + ijs.value().get_ref<const std::string&>();
      } else {
        THROW_Corruption("p2name[" + dbname + "].params[db_options|options] are all missing");
      }
      if (params_js.end() != (ijs = params_js.find("cf_options"))) {
        if (ijs.value().is_string())
          cfo_name = "json varname: " + ijs.value().get_ref<const std::string&>();
      } else {
        THROW_Corruption("p2name[" + dbname + "].params[cf_options|options] are all missing");
      }
    }
    bool html = JsonSmartBool(dump_options, "html", true);
    if (dbo_name.empty()) dbo_name = "json varname: (defined inline)";
    if (cfo_name.empty()) cfo_name = "json varname: (defined inline)";
    djs["DBOptions"][0] = dbo_name; dbo.SaveToJson(djs["DBOptions"][1], repo, html);
    djs["CFOptions"][0] = dbo_name; cfo.SaveToJson(djs["CFOptions"][1], repo, html);
    djs["CFOptions"][1]["MaxMemCompactionLevel"] = const_cast<DB&>(db).MaxMemCompactionLevel();
    djs["CFOptions"][1]["Level0StopWriteTrigger"] = const_cast<DB&>(db).Level0StopWriteTrigger();
    //Json_DB_Statistics(dbo.statistics.get(), djs, html);
    //Json_DB_IntProps(db, db.DefaultColumnFamily(), djs);
    //Json_DB_Level_Stats(db, db.DefaultColumnFamily(), djs, opt.num_levels, html);
    JS_Add_CFPropertiesWebView_Link(djs, db, html, repo);
    JS_RokcsDB_AddVersion(djs["version"], html);
    return JsonToString(djs, dump_options);
  }
};
static constexpr auto JS_DB_Manip = &PluginManipSingleton<DB_Manip>;

struct DB_MultiCF_Manip : PluginManipFunc<DB_MultiCF> {
  void Update(DB_MultiCF* db, const json& js,
              const SidePluginRepo& repo) const final {
    THROW_NotSupported("");
  }
  std::string ToString(const DB_MultiCF& db, const json& dump_options,
                       const SidePluginRepo& repo) const final {
    json djs;
    auto dbo = static_cast<DBOptions_Json&&>(db.db->GetDBOptions());
    const auto& dbmap = repo.m_impl->db;
    //const std::string& dbname = db.db->GetName();
    std::string dbname;
    {
      auto iter = repo.m_impl->db.p2name.find(db.db);
      if (repo.m_impl->db.p2name.end() == iter) {
        THROW_NotFound("db.p2name.find(), db.path = " + db.db->GetName());
      }
      dbname = iter->second.name;
    }
    if (dump_options.contains("compact")) {
      std::string cfname = dump_options["compact"];
      auto cfh = db.Get(cfname);
      if (nullptr == cfh) {
        THROW_NotFound("cf_name is not registered in repo, dbname = " + dbname
                       + ", cfname = " + cfname);
      }
      return RunManualCompact(db.db, cfh, dump_options);
    }
    auto i1 = dbmap.p2name.find(db.db);
    if (dbmap.p2name.end() == i1) {
      THROW_NotFound("db ptr is not registered in repo, dbname = " + dbname);
    }
    auto ijs = i1->second.params.find("params");
    if (i1->second.params.end() == ijs) {
      THROW_Corruption("p2name[" + dbname + "].params is missing");
    }
    std::string dbo_name;
    const json& params_js = ijs.value();
    if (params_js.end() != (ijs = params_js.find("db_options"))) {
      if (ijs.value().is_string()) {
        dbo_name = "json varname: " + ijs.value().get_ref<const std::string&>();
      }
    } else {
      THROW_Corruption("p2name[" + dbname + "].params[db_options|options] are all missing");
    }
    if (params_js.end() == (ijs = params_js.find("column_families"))) {
      THROW_Corruption("p2name[" + dbname + "].params.column_families are all missing");
    }
    const auto& def_cfo_js = ijs.value();
    bool html = JsonSmartBool(dump_options, "html", true);
    if (dbo_name.empty()) dbo_name = "json varname: (defined inline)";
    djs["CFOptions"]; // insert
    djs["CFProps"]; // insert
    djs["DBOptions"][0] = dbo_name;
    djs["path"] = db.db->GetName();
    dbo.SaveToJson(djs["DBOptions"][1], repo, html);
    auto& result_cfo_js = djs["CFOptions"];
    auto& cf_props = djs["CFProps"];
    for (size_t i = 0; i < db.cf_handles.size(); ++i) {
      ColumnFamilyHandle* cf = db.cf_handles[i];
      ColumnFamilyDescriptor cfd;
      cf->GetDescriptor(&cfd);
      const std::string& cf_name = cfd.name;
      auto cfo = static_cast<CFOptions_Json&>(static_cast<CFOptions&>(cfd.options));
      if (def_cfo_js.end() == (ijs = def_cfo_js.find(cf_name))) {
        THROW_Corruption(dbname + ".params.column_families." + cf_name + " is missing");
      }
      if (ijs.value().is_string()) {
        // find in repo.m_impl->cf_options
        auto cfo_varname = ijs.value().get_ref<const std::string&>();
        auto icf = IterPluginFind(repo.m_impl->cf_options, cfo_varname);
        if (repo.m_impl->cf_options.name2p->end() == icf) {
          THROW_Corruption("Missing cfo_varname = " + cfo_varname);
        }
        auto picf = repo.m_impl->cf_options.p2name.find(icf->second.get());
        if (repo.m_impl->cf_options.p2name.end() == picf) {
          THROW_Corruption("Missing cfo p2name, cfo_varname = " + cfo_varname);
        }
        if (html) {
          auto comment = "&nbsp;&nbsp;&nbsp;&nbsp; changed fields are shown below:";
          result_cfo_js[cf_name][0] = "json varname: " + JsonRepoGetHtml_ahref("CFOptions", cfo_varname) + comment;
        } else {
          result_cfo_js[cf_name][0] = "json varname: " + cfo_varname;
        }
        if (JsonSmartBool(dump_options, "full")) {
          result_cfo_js[cf_name][1] = picf->second.params["params"];
          // overwrite with up to date cfo
          cfo.SaveToJson(result_cfo_js[cf_name][1], repo, html);
        } else {
          json orig;  static_cast<const CFOptions_Json&>(*icf->second).SaveToJson(orig, repo, false);
          json jNew;  cfo.SaveToJson(jNew, repo, false);
          json hNew;  cfo.SaveToJson(hNew, repo, html);
          json diff = json::diff(orig, jNew);
          //fprintf(stderr, "CF %s: orig = %s\n", cf_name.c_str(), orig.dump(4).c_str());
          //fprintf(stderr, "CF %s: jNew = %s\n", cf_name.c_str(), jNew.dump(4).c_str());
          //fprintf(stderr, "CF %s: diff = %s\n", cf_name.c_str(), diff.dump(4).c_str());
          for (auto& kv : diff.items()) {
            kv.value()["op"] = "add";
          }
          json show = json().patch(diff);
          //fprintf(stderr, "CF %s: show = %s\n", cf_name.c_str(), show.dump(4).c_str());
          json jRes;
          for (auto& kv : show.items()) {
            jRes[kv.key()] = hNew[kv.key()];
          }
          if (IsDefaultPath(cfo.cf_paths, dbname)) {
            jRes.erase("cf_paths");
          }
          result_cfo_js[cf_name][1] = jRes;
        }
      }
      else { // ijs point to inline defined CFOptions
        result_cfo_js[cf_name][0] = "json varname: (defined inline)";
        result_cfo_js[cf_name][1] = ijs.value();
        //result_cfo_js[cf_name][1]["class"] = "CFOptions";
	      //result_cfo_js[cf_name][1]["params"] = ijs.value();
        cfo.SaveToJson(result_cfo_js[cf_name][1], repo, html);
      }
      result_cfo_js[cf_name][1]["MaxMemCompactionLevel"] = db.db->MaxMemCompactionLevel(cf);
      result_cfo_js[cf_name][1]["Level0StopWriteTrigger"] = db.db->Level0StopWriteTrigger(cf);
      //Json_DB_IntProps(*db.db, cf, cf_props[cf_name]);
      //Json_DB_Level_Stats(*db.db, cf, cf_props[cf_name], cfo.num_levels, html);
      JS_Add_CFPropertiesWebView_Link(cf_props[cf_name], html,
                                      dbname, cf_name, repo);
    }
    //Json_DB_Statistics(dbo.statistics.get(), djs, html);
    JS_RokcsDB_AddVersion(djs["version"], html);
    auto getStr = [](auto fn) -> std::string {
      std::string str;
      Status s = fn(str);
      if (s.ok())
        return str;
      else
        return "Fail: " + s.ToString();
    };
    using namespace std::placeholders;
    djs["DbIdentity"] = getStr(std::bind(&DB::GetDbIdentity, db.db, _1));
    djs["DbSessionId"] = getStr(std::bind(&DB::GetDbSessionId, db.db, _1));
    return JsonToString(djs, dump_options);
  }
};
static constexpr auto JS_DB_MultiCF_Manip = &PluginManipSingleton<DB_MultiCF_Manip>;

static
DB* JS_DB_Open(const json& js, const SidePluginRepo& repo) {
  std::string name, path;
  Options options(JS_Options(js, repo, &name, &path));
  bool read_only = false; // default false
  ROCKSDB_JSON_OPT_PROP(js, read_only);
  DB* db = nullptr;
  Status s;
  if (read_only)
    s = DB::OpenForReadOnly(options, path, &db);
  else
    s = DB::Open(options, path, &db);
  if (!s.ok())
    throw s; // NOLINT
  SetCFPropertiesWebView(db, name, repo);
  return db;
}
ROCKSDB_FACTORY_REG("DB::Open", JS_DB_Open);
ROCKSDB_FACTORY_REG("DB::Open", JS_DB_Manip);

static
DB* JS_DB_OpenForReadOnly(const json& js, const SidePluginRepo& repo) {
  std::string name, path;
  Options options(JS_Options(js, repo, &name, &path));
  bool error_if_log_file_exist = false; // default
  ROCKSDB_JSON_OPT_PROP(js, error_if_log_file_exist);
  DB* db = nullptr;
  Status s = DB::OpenForReadOnly(options, path, &db, error_if_log_file_exist);
  if (!s.ok())
    throw s;
  SetCFPropertiesWebView(db, name, repo);
  return db;
}
ROCKSDB_FACTORY_REG("DB::OpenForReadOnly", JS_DB_OpenForReadOnly);
ROCKSDB_FACTORY_REG("DB::OpenForReadOnly", JS_DB_Manip);

static void AutoCatchUpThreadProc(DB_MultiCF_Impl* impl) {
  DB* db = impl->db;
  std::string dbname = db->GetName();
  Env* env = Env::Default();
  long long t0 = env->NowNanos();
  //long long sleep_sum = 0;
  while (impl->m_catch_up_running) {
    Status s = db->TryCatchUpWithPrimary();
    if (!s.ok()) {
      fprintf(stderr, "ERROR: TryCatchUpWithPrimary(dbname=%s) = %s\n",
              dbname.c_str(), s.ToString().c_str());
    }
    long long t1 = env->NowNanos();
    long long dt = impl->m_catch_up_delay_ms * 1000000;
    if (t1 - t0 < dt) {
      using std::this_thread::sleep_for;
      sleep_for(std::chrono::nanoseconds(dt - (t1 - t0)));
      //sleep_sum += t1 - t0;
    }
    else {
      // do not sleep, catch up again immediately
    }
    t0 = t1;
  }
}
static void CreateCatchUpThread(DB_MultiCF* mcf) {
  auto p = static_cast<DB_MultiCF_Impl*>(mcf);
  p->m_catch_up_thread.reset(new std::thread(&AutoCatchUpThreadProc, p));
}

static
DB* JS_DB_OpenAsSecondary(const json& js, const SidePluginRepo& repo) {
  std::string name, path, secondary_path;
  ROCKSDB_JSON_REQ_PROP(js, secondary_path);
  Options options(JS_Options(js, repo, &name, &path));
  DB* db = nullptr;
  Status s = DB::OpenAsSecondary(options, path, secondary_path, &db);
  if (!s.ok())
    throw s;
  SetCFPropertiesWebView(db, name, repo);
  return db;
}
ROCKSDB_FACTORY_REG("DB::OpenAsSecondary", JS_DB_OpenAsSecondary);
ROCKSDB_FACTORY_REG("DB::OpenAsSecondary", JS_DB_Manip);

struct MultiCF_Open {
  std::shared_ptr<DBOptions> db_opt;
  std::vector<ColumnFamilyDescriptor> cfdvec;
  std::string name, path;
  const json* js_cf_desc;
  std::unique_ptr<DB_MultiCF_Impl> db;
  MultiCF_Open(const json& js, const SidePluginRepo& repo) {
    if (!js.is_object()) {
      THROW_InvalidArgument("param json must be an object");
    }
    auto iter = js.find("name");
    if (js.end() == iter) {
      THROW_InvalidArgument("missing param \"name\"");
    }
    name = iter.value().get_ref<const std::string&>();
    iter = js.find("path");
    if (js.end() == iter) {
      THROW_InvalidArgument("missing param \"path\", this should be a bug");
    }
    path = iter.value().get_ref<const std::string&>();
    iter = js.find("column_families");
    if (js.end() == iter) {
      THROW_InvalidArgument("missing param \"column_families\"");
    }
    js_cf_desc = &iter.value();
    iter = js.find("db_options");
    if (js.end() == iter) {
      THROW_InvalidArgument("missing param \"db_options\"");
    }
    auto& db_options_js = iter.value();
    db_opt = ROCKSDB_OBTAIN_OPT(db_options, db_options_js, repo);
    db.reset(new DB_MultiCF_Impl); // NOLINT
    db->m_repo = &repo;
    for (auto& kv : js_cf_desc->items()) {
      const std::string& cf_name = kv.key();
      auto& cf_js = kv.value();
      auto cf_options = ROCKSDB_OBTAIN_OPT(cf_options, cf_js, repo);
      cfdvec.emplace_back(cf_name, *cf_options);
    }
    if (cfdvec.empty()) {
      THROW_InvalidArgument("param \"column_families\" is empty");
    }
  }
};

static ColumnFamilyHandle*
JS_CreateCF(DB* db, const std::string& cfname,
            const ColumnFamilyOptions& cfo, const json&) {
  ColumnFamilyHandle* cfh = nullptr;
  Status s = db->CreateColumnFamily(cfo, cfname, &cfh);
  if (!s.ok()) {
    throw s; // NOLINT
  }
  return cfh;
}

static
DB_MultiCF* JS_DB_MultiCF_Open(const json& js, const SidePluginRepo& repo) {
  struct X : MultiCF_Open {
    X(const json& js, const SidePluginRepo& repo) : MultiCF_Open(js, repo) {
      bool read_only = false; // default false
      ROCKSDB_JSON_OPT_PROP(js, read_only);
      Status s;
      if (read_only)
        s = DB::OpenForReadOnly(
                     *db_opt, path, cfdvec, &db->cf_handles, &db->db);
      else
        s = DB::Open(*db_opt, path, cfdvec, &db->cf_handles, &db->db);
      if (!s.ok())
        throw s;
      if (db->cf_handles.size() != cfdvec.size())
        THROW_Corruption("cf_handles.size() != cfdvec.size()");
      SetCFPropertiesWebView(db.get(), name, cfdvec, repo);
      if (!read_only) {
        db->m_create_cf = &JS_CreateCF;
      }
      db->InitAddCF_ToMap(*js_cf_desc);
    }
  } p(js, repo);
  return p.db.release();
}
ROCKSDB_FACTORY_REG("DB::Open", JS_DB_MultiCF_Open);
ROCKSDB_FACTORY_REG("DB::Open", JS_DB_MultiCF_Manip);

static
DB_MultiCF*
JS_DB_MultiCF_OpenForReadOnly(const json& js, const SidePluginRepo& repo) {
  struct X : MultiCF_Open {
    X(const json& js, const SidePluginRepo& repo) : MultiCF_Open(js, repo) {
      bool error_if_log_file_exist = false;  // default is false
      ROCKSDB_JSON_OPT_PROP(js, error_if_log_file_exist);
      Status s = DB::OpenForReadOnly(*db_opt, path, cfdvec, &db->cf_handles,
                                     &db->db, error_if_log_file_exist);
      if (!s.ok())
        throw s;
      if (db->cf_handles.size() != cfdvec.size())
        THROW_Corruption("cf_handles.size() != cfdvec.size()");
      SetCFPropertiesWebView(db.get(), name, cfdvec, repo);
      db->InitAddCF_ToMap(*js_cf_desc);
    }
  } p(js, repo);
  return p.db.release();
}
ROCKSDB_FACTORY_REG("DB::OpenForReadOnly", JS_DB_MultiCF_OpenForReadOnly);
ROCKSDB_FACTORY_REG("DB::OpenForReadOnly", JS_DB_MultiCF_Manip);

static
DB_MultiCF*
JS_DB_MultiCF_OpenAsSecondary(const json& js, const SidePluginRepo& repo) {
  struct X : MultiCF_Open {
    X(const json& js, const SidePluginRepo& repo) : MultiCF_Open(js, repo) {
      std::string secondary_path;
      auto auto_catch_up_delay_ms = db->m_catch_up_delay_ms;
      ROCKSDB_JSON_REQ_PROP(js, secondary_path);
      ROCKSDB_JSON_OPT_PROP(js, auto_catch_up_delay_ms);
      Status s = DB::OpenAsSecondary(*db_opt, path, secondary_path, cfdvec,
                                     &db->cf_handles, &db->db);
      if (!s.ok())
        throw s;
      if (db->cf_handles.size() != cfdvec.size())
        THROW_Corruption("cf_handles.size() != cfdvec.size()");
      SetCFPropertiesWebView(db.get(), name, cfdvec, repo);
      db->m_create_cf = &JS_CreateCF;
      db->InitAddCF_ToMap(*js_cf_desc);
      db->m_catch_up_delay_ms = auto_catch_up_delay_ms;
      if (auto_catch_up_delay_ms > 0) {
        db->m_catch_up_running = true;
        CreateCatchUpThread(db.get());
      }
    }
  } p(js, repo);
  return p.db.release();
}
ROCKSDB_FACTORY_REG("DB::OpenAsSecondary", JS_DB_MultiCF_OpenAsSecondary);
ROCKSDB_FACTORY_REG("DB::OpenAsSecondary", JS_DB_MultiCF_Manip);

/////////////////////////////////////////////////////////////////////////////
// DBWithTTL::Open

static
DB* JS_DBWithTTL_Open(const json& js, const SidePluginRepo& repo) {
  std::string name, path;
  Options options(JS_Options(js, repo, &name, &path));
  int32_t ttl = 0; // default 0
  bool read_only = false; // default false
  ROCKSDB_JSON_OPT_PROP(js, ttl);
  ROCKSDB_JSON_OPT_PROP(js, read_only);
  DBWithTTL* db = nullptr;
  Status s = DBWithTTL::Open(options, path, &db, ttl, read_only);
  if (!s.ok())
    throw s;
  SetCFPropertiesWebView(db, name, repo);
  return db;
}
ROCKSDB_FACTORY_REG("DBWithTTL::Open", JS_DBWithTTL_Open);
ROCKSDB_FACTORY_REG("DBWithTTL::Open", JS_DB_Manip);

static ColumnFamilyHandle*
JS_CreateCF_WithTTL(DB* db, const std::string& cfname,
                    const ColumnFamilyOptions& cfo, const json& extra_args) {
  int ttl = 0;
  ROCKSDB_JSON_REQ_PROP(extra_args, ttl);
  ColumnFamilyHandle* cfh = nullptr;
  auto ttldb = dynamic_cast<DBWithTTL*>(db);
  Status s = ttldb->CreateColumnFamilyWithTtl(cfo, cfname, &cfh, ttl);
  if (!s.ok()) {
    throw s; // NOLINT
  }
  return cfh;
}
static
DB_MultiCF*
JS_DBWithTTL_MultiCF_Open(const json& js, const SidePluginRepo& repo) {
  struct X : MultiCF_Open {
    X(const json& js, const SidePluginRepo& repo) : MultiCF_Open(js, repo) {
      std::vector<int32_t> ttls;
      bool read_only = false;
      ROCKSDB_JSON_OPT_PROP(js, read_only);
      auto parse_ttl = [&ttls](const json& cf_js) {
        int32_t ttl = 0;
        ROCKSDB_JSON_REQ_PROP(cf_js, ttl);
        ttls.push_back(ttl);
      };
      for (auto& item : js_cf_desc->items()) {
        parse_ttl(item.value());
      }
      DBWithTTL* dbptr = nullptr;
      Status s = DBWithTTL::Open(*db_opt, path, cfdvec,
                                 &db->cf_handles, &dbptr, ttls, read_only);
      if (!s.ok())
        throw s;  // NOLINT
      if (db->cf_handles.size() != cfdvec.size())
        THROW_Corruption("cf_handles.size() != cfdvec.size()");
      db->db = dbptr;
      SetCFPropertiesWebView(db.get(), name, cfdvec, repo);
      db->m_create_cf = &JS_CreateCF_WithTTL;
      db->InitAddCF_ToMap(*js_cf_desc);
    }
  } p(js, repo);
  return p.db.release();
}
ROCKSDB_FACTORY_REG("DBWithTTL::Open", JS_DBWithTTL_MultiCF_Open);
ROCKSDB_FACTORY_REG("DBWithTTL::Open", JS_DB_MultiCF_Manip);

/////////////////////////////////////////////////////////////////////////////
// TransactionDB::Open
static std::shared_ptr<TransactionDBMutexFactory>
JS_NewTransactionDBMutexFactoryImpl(const json&, const SidePluginRepo&) {
  return std::make_shared<TransactionDBMutexFactoryImpl>();
}
ROCKSDB_FACTORY_REG("Default", JS_NewTransactionDBMutexFactoryImpl);
ROCKSDB_FACTORY_REG("default", JS_NewTransactionDBMutexFactoryImpl);
ROCKSDB_FACTORY_REG("TransactionDBMutexFactoryImpl", JS_NewTransactionDBMutexFactoryImpl);

struct TransactionDBOptions_Json : TransactionDBOptions {
  TransactionDBOptions_Json(const json& js, const SidePluginRepo& repo) {
    ROCKSDB_JSON_OPT_PROP(js, max_num_locks);
    ROCKSDB_JSON_OPT_PROP(js, max_num_deadlocks);
    ROCKSDB_JSON_OPT_PROP(js, num_stripes);
    ROCKSDB_JSON_OPT_PROP(js, transaction_lock_timeout);
    ROCKSDB_JSON_OPT_PROP(js, default_lock_timeout);
    ROCKSDB_JSON_OPT_FACT(js, custom_mutex_factory);
    ROCKSDB_JSON_OPT_ENUM(js, write_policy);
    ROCKSDB_JSON_OPT_PROP(js, rollback_merge_operands);
    ROCKSDB_JSON_OPT_PROP(js, skip_concurrency_control);
    ROCKSDB_JSON_OPT_PROP(js, default_write_batch_flush_threshold);
  }
};

TransactionDBOptions
JS_TransactionDBOptions(const json& js, const SidePluginRepo& repo) {
  auto iter = js.find("txn_db_options");
  if (js.end() == iter) {
    THROW_InvalidArgument("missing required param \"txn_db_options\"");
  }
  return TransactionDBOptions_Json(iter.value(), repo);
}

static
DB* JS_TransactionDB_Open(const json& js, const SidePluginRepo& repo) {
  std::string name, path;
  Options options(JS_Options(js, repo, &name, &path));
  TransactionDBOptions trx_db_options(JS_TransactionDBOptions(js, repo));
  TransactionDB* db = nullptr;
  Status s = TransactionDB::Open(options, trx_db_options, path, &db);
  if (!s.ok())
    throw s;
  SetCFPropertiesWebView(db, name, repo);
  return db;
}
ROCKSDB_FACTORY_REG("TransactionDB::Open", JS_TransactionDB_Open);
ROCKSDB_FACTORY_REG("TransactionDB::Open", JS_DB_Manip);

static
DB_MultiCF*
JS_TransactionDB_MultiCF_Open(const json& js, const SidePluginRepo& repo) {
  struct X : MultiCF_Open {
    X(const json& js, const SidePluginRepo& repo) : MultiCF_Open(js, repo) {
      TransactionDBOptions trx_db_options(JS_TransactionDBOptions(js, repo));
      TransactionDB* dbptr = nullptr;
      Status s = TransactionDB::Open(*db_opt, trx_db_options, path, cfdvec,
                                     &db->cf_handles, &dbptr);
      if (!s.ok())
        throw s;
      if (db->cf_handles.size() != cfdvec.size())
        THROW_Corruption("cf_handles.size() != cfdvec.size()");
      db->db = dbptr;
      SetCFPropertiesWebView(db.get(), name, cfdvec, repo);
      db->m_create_cf = &JS_CreateCF;
      db->InitAddCF_ToMap(*js_cf_desc);
    }
  } p(js, repo);
  return p.db.release();
}
ROCKSDB_FACTORY_REG("TransactionDB::Open", JS_TransactionDB_MultiCF_Open);
ROCKSDB_FACTORY_REG("TransactionDB::Open", JS_DB_MultiCF_Manip);

/////////////////////////////////////////////////////////////////////////////
struct OptimisticTransactionDBOptions_Json: OptimisticTransactionDBOptions {
  OptimisticTransactionDBOptions_Json(const json& js, const SidePluginRepo&) {
    ROCKSDB_JSON_OPT_ENUM(js, validate_policy);
    ROCKSDB_JSON_OPT_PROP(js, occ_lock_buckets);
  }
};
static
DB* JS_OccTransactionDB_Open(const json& js, const SidePluginRepo& repo) {
  std::string name, path;
  Options options(JS_Options(js, repo, &name, &path));
  OptimisticTransactionDB* db = nullptr;
  Status s = OptimisticTransactionDB::Open(options, path, &db);
  if (!s.ok())
    throw s;
  SetCFPropertiesWebView(db, name, repo);
  return db;
}
ROCKSDB_FACTORY_REG("OptimisticTransactionDB::Open", JS_OccTransactionDB_Open);
ROCKSDB_FACTORY_REG("OptimisticTransactionDB::Open", JS_DB_Manip);

static
DB_MultiCF*
JS_OccTransactionDB_MultiCF_Open(const json& js, const SidePluginRepo& repo) {
  struct X : MultiCF_Open {
    X(const json& js, const SidePluginRepo& repo) : MultiCF_Open(js, repo) {
      OptimisticTransactionDB* dbptr = nullptr;
      auto iter = js.find("occ_options");
      if (js.end() == iter) {
        Status s = OptimisticTransactionDB::Open(
            *db_opt, path, cfdvec, &db->cf_handles, &dbptr);
        if (!s.ok())
          throw s;
      }
      else {
        Status s = OptimisticTransactionDB::Open(
            *db_opt, OptimisticTransactionDBOptions_Json(iter.value(), repo),
            path, cfdvec, &db->cf_handles, &dbptr);
        if (!s.ok())
          throw s;
      }
      if (db->cf_handles.size() != cfdvec.size())
        THROW_Corruption("cf_handles.size() != cfdvec.size()");
      db->db = dbptr;
      SetCFPropertiesWebView(db.get(), name, cfdvec, repo);
      db->m_create_cf = &JS_CreateCF;
      db->InitAddCF_ToMap(*js_cf_desc);
    }
  } p(js, repo);
  return p.db.release();
}
ROCKSDB_FACTORY_REG("OptimisticTransactionDB::Open", JS_OccTransactionDB_MultiCF_Open);
ROCKSDB_FACTORY_REG("OptimisticTransactionDB::Open", JS_DB_MultiCF_Manip);

/////////////////////////////////////////////////////////////////////////////
// BlobDB::Open
namespace blob_db {

struct BlobDBOptions_Json : BlobDBOptions {
  BlobDBOptions_Json(const json& js, const SidePluginRepo&) {
    ROCKSDB_JSON_OPT_PROP(js, blob_dir);
    ROCKSDB_JSON_OPT_PROP(js, path_relative);
    ROCKSDB_JSON_OPT_PROP(js, is_fifo);
    ROCKSDB_JSON_OPT_SIZE(js, max_db_size);
    ROCKSDB_JSON_OPT_PROP(js, ttl_range_secs);
    ROCKSDB_JSON_OPT_SIZE(js, min_blob_size);
    ROCKSDB_JSON_OPT_SIZE(js, bytes_per_sync);
    ROCKSDB_JSON_OPT_SIZE(js, blob_file_size);
    ROCKSDB_JSON_OPT_ENUM(js, compression);
    ROCKSDB_JSON_OPT_PROP(js, enable_garbage_collection);
    ROCKSDB_JSON_OPT_PROP(js, garbage_collection_cutoff);
    ROCKSDB_JSON_OPT_PROP(js, disable_background_tasks);
  }
  json ToJson(const SidePluginRepo&) const {
    json js;
    ROCKSDB_JSON_SET_PROP(js, blob_dir);
    ROCKSDB_JSON_SET_PROP(js, path_relative);
    ROCKSDB_JSON_SET_PROP(js, is_fifo);
    ROCKSDB_JSON_SET_SIZE(js, max_db_size);
    ROCKSDB_JSON_SET_PROP(js, ttl_range_secs);
    ROCKSDB_JSON_SET_SIZE(js, min_blob_size);
    ROCKSDB_JSON_SET_SIZE(js, bytes_per_sync);
    ROCKSDB_JSON_SET_SIZE(js, blob_file_size);
    ROCKSDB_JSON_SET_ENUM(js, compression);
    ROCKSDB_JSON_SET_PROP(js, enable_garbage_collection);
    ROCKSDB_JSON_SET_PROP(js, garbage_collection_cutoff);
    ROCKSDB_JSON_SET_PROP(js, disable_background_tasks);
    return js;
  }
};

BlobDBOptions
JS_BlobDBOptions(const json& js, const SidePluginRepo& repo) {
  auto iter = js.find("bdb_options");
  if (js.end() == iter) {
    THROW_InvalidArgument("missing required param \"bdb_options\"");
  }
  return BlobDBOptions_Json(iter.value(), repo);
}

static
DB* JS_BlobDB_Open(const json& js, const SidePluginRepo& repo) {
  std::string name, path;
  Options options(JS_Options(js, repo, &name, &path));
  BlobDBOptions bdb_options(JS_BlobDBOptions(js, repo));
  BlobDB* db = nullptr;
  Status s = BlobDB::Open(options, bdb_options, path, &db);
  if (!s.ok())
    throw s;
  SetCFPropertiesWebView(db, name, repo);
  return db;
}
ROCKSDB_FACTORY_REG("BlobDB::Open", JS_BlobDB_Open);
ROCKSDB_FACTORY_REG("BlobDB::Open", JS_DB_Manip);

static
DB_MultiCF*
JS_BlobDB_MultiCF_Open(const json& js, const SidePluginRepo& repo) {
  struct X : MultiCF_Open {
    X(const json& js, const SidePluginRepo& repo) : MultiCF_Open(js, repo) {
      BlobDBOptions bdb_options(JS_BlobDBOptions(js, repo));
      BlobDB* dbptr = nullptr;
      Status s = BlobDB::Open(*db_opt, bdb_options, path, cfdvec,
                        &db->cf_handles, &dbptr);
      if (!s.ok())
        throw s;
      if (db->cf_handles.size() != cfdvec.size())
        THROW_Corruption("cf_handles.size() != cfdvec.size()");
      db->db = dbptr;
      SetCFPropertiesWebView(db.get(), name, cfdvec, repo);
      db->m_create_cf = &JS_CreateCF;
      db->InitAddCF_ToMap(*js_cf_desc);
    }
  } p(js, repo);
  return p.db.release();
}
ROCKSDB_FACTORY_REG("BlobDB::Open", JS_BlobDB_MultiCF_Open);
ROCKSDB_FACTORY_REG("BlobDB::Open", JS_DB_MultiCF_Manip);

} // namespace blob_db

DB_MultiCF::DB_MultiCF() = default;
DB_MultiCF::~DB_MultiCF() = default;

// users should ensure databases are alive when calling this function
void SidePluginRepo::CloseAllDB(bool del_rocksdb_objs) {
  m_impl->http.Close();
#if 1
  using view_kv_ptr = decltype(&*m_impl->props.p2name.cbegin());
  //using view_kv_ptr = const std::pair<const void* const, Impl::ObjInfo>*;
  std::map<const void*, view_kv_ptr> cfh_to_view;
  for (auto& kv : m_impl->props.p2name) {
    auto view = (CFPropertiesWebView*)kv.first;
    cfh_to_view[view->cfh] = &kv;
  }
  auto del_view = [&](ColumnFamilyHandle* cfh) {
    auto iter = cfh_to_view.find(cfh);
    assert(cfh_to_view.end() != iter);
    ROCKSDB_VERIFY_F(cfh_to_view.end() != iter, "cfh must in cfh_to_view");
    auto view = (CFPropertiesWebView*)(iter->second->first);
    const Impl::ObjInfo& oi = iter->second->second;
    m_impl->props.name2p->erase(oi.name);
    m_impl->props.p2name.erase(view); // this erase invalidate 'oi'
  };
#else
  auto del_view = [&](ColumnFamilyHandle*) {};
#endif
  auto del_rocks = [del_rocksdb_objs](auto obj) {
    if (del_rocksdb_objs)
      delete obj;
  };
  for (const auto& kv : *m_impl->db.name2p) {
    assert(nullptr != kv.second.db);
    if (kv.second.dbm) {
      DB_MultiCF* dbm = kv.second.dbm;
      assert(kv.second.db == dbm->db);
      auto idbm = static_cast<DB_MultiCF_Impl*>(dbm);
      if (idbm->m_catch_up_thread) {
        idbm->m_catch_up_running = false; // notify stop
        idbm->m_catch_up_thread->join();
        idbm->m_catch_up_thread.reset();
      }
      for (auto cfh : dbm->cf_handles) {
        del_view(cfh);
        del_rocks(cfh);
      }
      del_rocks(dbm->db);
      delete dbm;
    }
    else {
      DB* db = kv.second.db;
      del_view(db->DefaultColumnFamily());
      del_rocks(db);
    }
  }
  m_impl->db.name2p->clear();
  m_impl->db.p2name.clear();
}

}
