#include <rocksdb/rocksdb_namespace.h>
#include <unordered_map>
#include "toml.hpp"

namespace ROCKSDB_NAMESPACE {
std::string TomlToJson(std::string_view doc, std::string_view source_path) {
  auto toml_obj = toml::parse(doc, source_path);
  std::ostringstream oss;
  oss << toml::json_formatter{toml_obj};
  return oss.str();
}
} // namespace ROCKSDB_NAMESPACE
