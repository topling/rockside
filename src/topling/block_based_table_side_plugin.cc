#include <string>

#include "table/block_based/block_based_table_reader.h"
#include "db/column_family.h"
#include <topling/side_plugin_factory.h>

namespace ROCKSDB_NAMESPACE {

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

void Html_AppendInternalKey(std::string& html, Slice ikey, const UserKeyCoder*);

class AccessBlockBasedTable : public BlockBasedTable {
public:
std::string BlockIndexHtml(ColumnFamilyData* cfd) const {
  char buf[64];
  std::string html;
  const bool index_key_includes_seq = rep_->index_key_includes_seq;
  const bool index_has_first_key = rep_->index_has_first_key;
  html.append("<h4>DataBlock Index</h4>");
  {
    html.append(R"(
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
    .part_user_key {
      color: DarkBlue;
    }
    .hex_key {
      color: DarkRed
    }
  </style>
)");
  }
  if (index_key_includes_seq) {
    if (index_has_first_key) {
      html.append(R"(
<table border=1>
</thead>
  <tr>
    <th rowspan=2>Ord</th>
    <th rowspan=2>Offset</th>
    <th rowspan=2>Size</th>
    <th colspan=3>IndexKey</th>
    <th colspan=3>FirstKey</th>
  </tr>
  <tr>
    <th>UserKey</th>
    <th>Seq</th>
    <th>Type</th>
    <th>UserKey</th>
    <th>Seq</th>
    <th>Type</th>
  </tr>
</thead>
<tbody>
)");
    }
    else {
      html.append(R"(
<table border=1>
</thead>
  <tr>
    <th rowspan=2>Ord</th>
    <th rowspan=2>Offset</th>
    <th rowspan=2>Size</th>
    <th colspan=3>IndexKey</th>
  </tr>
  <tr>
    <th>UserKey</th>
    <th>Seq</th>
    <th>Type</th>
  </tr>
</thead>
<tbody>
)");
    }
  }
  else {
    if (index_has_first_key) {
      html.append(R"(
<table border=1>
</thead>
  <tr>
    <th>Ord</th>
    <th>Offset</th>
    <th>Size</th>
    <th>IndexUserKey</th>
    <th>FirstUserKey</th>
  </tr>
</thead>
<tbody>
)");
    }
    else {
      html.append(R"(
<table border=1>
</thead>
  <tr>
    <th>Ord</th>
    <th>Offset</th>
    <th>Size</th>
    <th>IndexUserKey</th>
  </tr>
</thead>
<tbody>
)");
    }
  }
  auto coder_sp = cfd->GetLatestCFOptions().html_user_key_coder;
  const UserKeyCoder* coder = nullptr;
/*
  const bool is_full_user_key =
     BlockBasedTableOptions::IndexShorteningMode::kNoShortening ==
     rep_->table_options.index_shortening;\
*/
  if (coder_sp) {
  //  if (is_full_user_key)
      // when using coder, index_key must be a full user_key
      coder = dynamic_cast<const UserKeyCoder*>(coder_sp.get());
  }
  bool disable_prefix_seek = false;
  BlockCacheLookupContext lookup_context{TableReaderCaller::kPrefetch};
  std::unique_ptr<InternalIteratorBase<IndexValue>> index_iter(NewIndexIterator(
      ReadOptions(), disable_prefix_seek,
      /*input_iter=*/nullptr, /*get_context=*/nullptr, &lookup_context));
  size_t num = 0;
  index_iter->SeekToFirst();
  for (; index_iter->Valid(); index_iter->Next(), num++) {
    Slice index_key = index_iter->key();
    IndexValue iv = index_iter->value();
    size_t offset = size_t(iv.handle.offset());
    size_t size = size_t(iv.handle.size());
    html.append("<tr>");
    html.append(buf, sprintf(buf, "<td align=right>%zd</td>", num));
    html.append(buf, sprintf(buf, "<td align=right>%zd</td>", offset));
    html.append(buf, sprintf(buf, "<td align=right>%zd</td>", size));
    if (index_key_includes_seq) {
      // coder must handle part key
      Html_AppendInternalKey(html, index_key, coder);
    }
    else {
     #if 0 // coder do not handle part key
      html.append("<td class='monoleft");
      if (coder) {
        html.append("'>");
        html.append(coder->Decode(index_key));
      } else if (std::all_of(index_key.begin(), index_key.end(), isprint)) {
        if (!is_full_user_key) html.append(" part_user_key");
        html.append("'>");
        html.append(index_key.data_, index_key.size_);
      } else if (std::count_if(index_key.begin(), index_key.end(), isprint) >= index_key.size()*7/8) {
        if (!is_full_user_key) html.append(" part_user_key");
        html.append("'>");
        HtmlAppendEscape(&html, index_key);
      } else { // non print-able is too many
        html.append(" hex_key'>");
        html.append(Slice(index_key).ToString(true));
      }
     #else
      html.append("<td class='monoleft'>");
      if (coder)
        html.append(coder->Decode(index_key));
      else
        html.append(Slice(index_key).ToString(true));
     #endif
      html.append("</td>");
    }
    if (index_has_first_key) {
      Html_AppendInternalKey(html, iv.first_internal_key, coder);
    }
    html.append("</tr>\n");
  }
/*
  // num == rep_->table_properties->num_data_blocks;
  return std::string(buf, sprintf(buf, "<h4>Entries: %zd</h4>\n", num)) +
          std::move(html);
*/
  return html;
}

json ToWebViewJson(const json& dump_options) const {
  json djs;
  const BlockBasedTable::Rep* r = get_rep();

  djs["global_seqno"] = (int64_t)r->global_seqno;
  djs["level"] = r->level;
  djs["cf_id"] = r->table_properties->column_family_id;
  djs["cf_name"] = r->table_properties->column_family_name;

  json data = json::object({
      {"size", r->table_properties->data_size},
      {"num_blocks", r->table_properties->num_data_blocks},
  });
  json index = json::object({
      {"total_size", r->table_properties->index_size},
      {"index_type", enum_stdstr(r->index_type)},
      {"index_has_first_key", r->index_has_first_key},
      {"index_key_includes_seq", r->index_key_includes_seq},
      {"index_value_is_full", r->index_value_is_full},
      //{"entries", "none"},
      {"ApproximateMemoryUsage",
         r->index_reader ? json(r->index_reader->ApproximateMemoryUsage())
                         : json("nullptr") }
  });
  if (BlockBasedTableOptions::kHashSearch == r->index_type) {
    index["hash_index_allow_collision"] = r->hash_index_allow_collision;
  }
  json filter = json::object({
      {"filter_type", enum_stdstr(r->filter_type)},
      {"total_size", r->table_properties->filter_size},
      {"whole_key_filtering", r->whole_key_filtering},
      {"prefix_filtering", r->prefix_filtering},
      //{"entries", "none"},
      {"ApproximateMemoryUsage",
         r->filter ? json(r->filter->ApproximateMemoryUsage())
                   : json("nullptr") }
  });
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
      {"k/(k+v)", (double)r->table_properties->raw_key_size /
                  (r->table_properties->raw_key_size +
                   r->table_properties->raw_value_size)},
      {"total_size",
       r->table_properties->raw_key_size + r->table_properties->raw_value_size},
  });


  djs["ApproximateMemoryUsage"] = ApproximateMemoryUsage();
  if (r->fragmented_range_dels)
    djs["num_unfragmented_tombstones"] =
          r->fragmented_range_dels->num_unfragmented_tombstones();
  djs["blocks_maybe_compressed"] = r->blocks_maybe_compressed;
  djs["blocks_definitely_zstd_compressed"] = r->blocks_definitely_zstd_compressed;

  auto cfd = (ColumnFamilyData*)(dump_options["__ptr_cfd__"].get<size_t>());
  djs["BlockIndex"] = BlockIndexHtml(cfd);

  return djs;
}
}; // AccessBlockBasedTable

static
std::string ToWebViewString(const BlockBasedTable* reader,
                            const json& dump_options) {
  auto acc = static_cast<const AccessBlockBasedTable*>(reader);
  auto djs = acc->ToWebViewJson(dump_options);
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

} // namespace ROCKSDB_NAMESPACE
