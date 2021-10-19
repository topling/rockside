#include <string>

#include "table/block_based/block_based_table_reader.h"
#include <topling/side_plugin_factory.h>

using namespace ROCKSDB_NAMESPACE;

// erase all space in string s
void CleanSpace(std::string& s) {
  size_t s_len, i;
  for (s_len = 0, i = 0; i < s.size(); ++i) {
    if (s[i] != ' ') {
      s[s_len++] = s[i];
    }
  }
  s.resize(s_len);
}

// s contains some keys and values
json SliceStringIntoJson(std::string s) {
  json rtn;
  int pre_index = 0;
  std::string temp_key;

  CleanSpace(s);
  for (size_t i = 0; i < s.size(); ++i) {
    if (s[i] == '=') {
      temp_key = s.substr(pre_index, i - pre_index);
      pre_index = i + 1;
    } else if (s[i] == ';') {
      // rtn[temp_key] = (int16_t)std::stoi(s.substr(pre_index, i - pre_index));
      rtn[temp_key] = s.substr(pre_index, i - pre_index);
      pre_index = i + 1;
    }
  }

  return rtn;
}

// transfer uint64 time value to string
std::string Uint64ToTimeString(uint64_t tparam) {
  time_t t = (time_t)tparam;
  return ctime(&t);
}

json ToWebViewJson(const BlockBasedTable* reader, const json& dump_options) {
  json djs;
  const BlockBasedTable::Rep* r = reader->get_rep();

  djs["global_seqno"] = (int64_t)r->global_seqno;

  json data = json::object({
      {"size", r->table_properties->data_size},
      {"num_blocks", r->table_properties->num_data_blocks},
  });
  json index = json::object({
      {"total_size", r->table_properties->index_size},
      {"entries", "none"},
  });
  if (r->index_reader != nullptr) {
    index.push_back({"index_ApproximateMemoryUsage", r->index_reader->ApproximateMemoryUsage()});
  } else {
    index.push_back({"index_ApproximateMemoryUsage", "nullptr"});
  }
  json filter = json::object({
      {"total_size", r->table_properties->filter_size},
      {"entries", "none"},
  });
  if (r->filter != nullptr) {
    filter.push_back({"filter_ApproximateMemoryUsage", r->filter->ApproximateMemoryUsage()});
  } else {
    filter.push_back({"filter_ApproximateMemoryUsage", "nullptr"});
  }
  djs["blocks"] = json::object({
      {"data", data},
      {"index", index},
      {"filter", filter},
  });
  djs["time"] = json::object({
      {"creation_time", Uint64ToTimeString(r->table_properties->creation_time)},
      {"file_creation_time",
       Uint64ToTimeString(r->table_properties->file_creation_time)},
  });
  djs["compression"] = json::object({
      {"name", r->table_properties->compression_name},
      {"options",
       SliceStringIntoJson(r->table_properties->compression_options)},
      {"ratio", (double)(r->table_properties->raw_key_size +
                         r->table_properties->raw_value_size) /
                    r->file_size},
      {"file_size", r->file_size},
  });

  djs["kv_info"]["raw"] = json::object({
      {"key_size", r->table_properties->raw_key_size},
      {"value_size", r->table_properties->raw_value_size},
      {"k/v", (double)r->table_properties->raw_key_size /
                  r->table_properties->raw_value_size},
      {"total_size",
       r->table_properties->raw_key_size + r->table_properties->raw_value_size},
  });


  djs["ApproximateMemoryUsage"] = reader->ApproximateMemoryUsage();

  return djs;
}

std::string ToWebViewString(const BlockBasedTable* reader,
                            const json& dump_options) {
  auto djs = ToWebViewJson(reader, dump_options);
  // return R"EOS(<style>
  //       table {
  //           background-image:
  //           url(https://raw.githubusercontent.com/gukaifeng/PicGo/master/img/avatar.png);
  //           color: white;
  //       }
  //   </style>)EOS" + JsonToString(djs, dump_options);
  return JsonToString(djs, dump_options);
}

struct BlockBasedTableReader_Manip : PluginManipFunc<TableReader> {
  void Update(TableReader*, const json& js,
              const SidePluginRepo &repo) const final {}

  std::string ToString(const TableReader& reader, const json& dump_options,
                       const SidePluginRepo &) const final {
    // return ToWebViewString(dump_options);
    if (auto rd = dynamic_cast<const BlockBasedTable*>(&reader)) {
      return ToWebViewString(rd, dump_options);
    }

    THROW_InvalidArgument("Unknow TableReader");
  }
};

ROCKSDB_REG_PluginManip("NoCompression", BlockBasedTableReader_Manip);
ROCKSDB_REG_PluginManip("Snappy", BlockBasedTableReader_Manip);
ROCKSDB_REG_PluginManip("Zlib", BlockBasedTableReader_Manip);
ROCKSDB_REG_PluginManip("BZip2", BlockBasedTableReader_Manip);
ROCKSDB_REG_PluginManip("LZ4", BlockBasedTableReader_Manip);
ROCKSDB_REG_PluginManip("LZ4HC", BlockBasedTableReader_Manip);
ROCKSDB_REG_PluginManip("Xpress", BlockBasedTableReader_Manip);
ROCKSDB_REG_PluginManip("ZSTD", BlockBasedTableReader_Manip);
ROCKSDB_REG_PluginManip("ZSTDNotFinal", BlockBasedTableReader_Manip);