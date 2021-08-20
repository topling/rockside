//
// Created by leipeng on 2020/8/18.
//
#pragma once

#include <rocksdb/slice.h>
#include <topling/json_fwd.h>

struct mg_connection;
namespace ROCKSDB_NAMESPACE {

using nlohmann::json;
class SidePluginRepo;
class JsonCivetServer {
public:
  ~JsonCivetServer();
  JsonCivetServer();
  void Init(const json& conf, SidePluginRepo*);
  void Close();
  JsonCivetServer(const JsonCivetServer&) = delete;
  JsonCivetServer& operator=(const JsonCivetServer&) = delete;
private:
  class Impl;
  Impl* m_impl;
};

int mg_write(mg_connection*, Slice);

} // ROCKSDB_NAMESPACE
