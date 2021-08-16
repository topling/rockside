//
// Created by leipeng on 2020/8/18.
//

#include "CivetServer.h"
#include "json_civetweb.h"
#include <topling/side_plugin_factory.h>
#include <chrono>

namespace ROCKSDB_NAMESPACE {

json from_query_string(const char* qry) {
  json js;
  if (!qry)
    return js;
  const char* end = qry + strlen(qry);
  while (qry < end) {
    const char* sep = std::find(qry, end, '&');
    const char* eq = std::find(qry, sep, '=');
    std::string name(qry, eq);
    auto& value_ref = js[name];
    std::string value;
    if (eq != sep)
      value.assign(eq+1, sep);
    if (value_ref.is_null())
      value_ref = std::move(value);
    else if (value_ref.is_string()){
      value_ref = json::array({value_ref, value});
    }
    else if (value_ref.is_array()) {
      value_ref.push_back(value);
    }
    else {
      ROCKSDB_DIE("should not goes here");
    }
    qry = sep + 1;
  }
  return js;
}

static time_t g_web_start_time = ::time(NULL); // NOLINT

std::string cur_time_stat() {
  char buf[64];
  time_t rawtime;
  time(&rawtime);
  struct tm  result;
  struct tm* timeinfo = localtime_r(&rawtime, &result);
  strftime(buf, sizeof(buf), "%F %T", timeinfo);
  size_t sec = (size_t)difftime(rawtime, g_web_start_time);
  size_t days = sec / 86400; sec %= 86400;
  size_t hours = sec / 3600; sec %= 3600;
  size_t minites = sec / 60; sec %= 60;
  std::string str; str.resize(256);
  str.resize(snprintf(&str[0], str.size(), "%s , Up: %zd-%02zd:%02zd:%02zd",
                      buf, days, hours, minites, sec));
  return str;
}

static std::string& operator|(std::string& str, Slice x) {
  str.append(x.data_, x.size_);
  return str;
}
void mg_print_cur_time(mg_connection* conn, const json& query,
                       const SidePluginRepo* repo) {
  std::string str;
  str.reserve(4096);
  std::string tm_str = cur_time_stat();
  const char* comma = (const char*)memchr(tm_str.data(), ',', tm_str.size());
  str|"<p id='time_stat_line'>";
  str|"<a href='javascript:SetParam(`refresh`,`3`)'>";
  str.append(tm_str.data(), comma - 1 - tm_str.data());
  str|"</a>";
  str|" , ";
  str|"<a href='javascript:SetParam(`refresh`,`0`)'>Up</a>: ";
  str|"<a href='javascript:SetParam(`refresh`,`1`)'>";
  str.append(comma + 6);
  str|"</a>";
  if (repo) for (auto& kvp : *repo->GetAllDB()) {
    const std::string& dbname = kvp.first; // not dbpath
    const DB_Ptr& dbp = kvp.second;
    ROCKSDB_VERIFY(nullptr != dbp.db);
    str | "&nbsp;&nbsp;&nbsp;";
    str | "<a href='/" | dbname | "/'>" | dbname | "</a>/";
    str | "<a href='/" | dbname | "/LOG'>LOG</a>";
  }
  str|"</p>";
  mg_write(conn, str.data(), str.size());
}
void mg_print_cur_time(mg_connection* conn, const json& query) {
  mg_print_cur_time(conn, query, nullptr);
}

std::string ReadPostData(mg_connection* conn) {
  std::string post; post.resize(8192);
  size_t pos = 0;
  while (true) {
    if (pos + 4096 < post.size()) {
      post.resize(pos + 4096);
    }
    auto len = mg_read(conn, &post[pos], post.size() - pos);
    if (len > 0) {
      pos += len;
    }
    if (0 == len)
      break; // have read all data
  }
  post.resize(pos);
  return post;
}

template<class Ptr>
class RepoHandler : public CivetHandler {
public:
  SidePluginRepo* m_repo;
  SidePluginRepo::Impl::ObjMap<Ptr>* m_map;
  Slice m_ns;

  RepoHandler(const char* clazz,
              SidePluginRepo* repo,
              SidePluginRepo::Impl::ObjMap<Ptr>* map) {
    m_repo = repo;
    m_ns = clazz;
    m_map = map;
    if (SidePluginRepo::DebugLevel() >= 2) {
      fprintf(stderr, "INFO: http: clazz: %s\n", clazz);
    }
  }

  bool handleGet(CivetServer *server, struct mg_connection *conn) override {
    mg_printf(conn,
              "HTTP/1.1 200 OK\r\n"
              "Content-Type: text/html; charset=utf-8\r\n"
              //"Connection: close\r\n"
              "\r\n");

    const mg_request_info* req = mg_get_request_info(conn);
    json query = from_query_string(req->query_string);
    const char* uri = req->local_uri;
    if (nullptr == uri) {
      mg_printf(conn, "ERROR: local uri is null\r\n");
      return true;
    }
    const bool html = JsonSmartBool(query, "html", true);
    while ('/' == *uri) uri++;
    size_t urilen = strlen(uri);
    auto slash = (const char*)memchr(uri, '/', urilen);
    if (NULL == slash) {
      std::vector<std::pair<std::string, Ptr> > vec;
      vec.reserve(m_map->name2p->size());
      vec.assign(m_map->name2p->begin(), m_map->name2p->end());
      std::sort(vec.begin(), vec.end());
      if (!html) {
        json djs;
        for (auto& x : vec) {
          djs.push_back(x.first);
        }
        std::string jstr = djs.dump();
        mg_write(conn, jstr.data(), jstr.size());
      }
      else {
        mg_printf(conn, "<html><title>%s</title>\n<body>\n", m_ns.data_);
        mg_print_cur_time(conn, query, m_repo);
        if (vec.empty()) {
          mg_printf(conn, "<strong>%s</strong> repo is empty</body></html>\n",
                    m_ns.data_);
        }
        else {
          mg_printf(conn, "<table border=1><tbody>\n"
                    "<tr align='left'><th>name</th><th>class</th></tr>\n");
          for (auto& kv : vec) {
            const auto name = kv.first.c_str();
            const auto pobj = GetRawPtr(kv.second);
            const auto iter = m_map->p2name.find(pobj);
            ROCKSDB_VERIFY_F(iter != m_map->p2name.end(), "%s : %s", name, m_ns.data_);
            const SidePluginRepo::Impl::ObjInfo& obj_info = iter->second;
            const auto jter = obj_info.params.find("class");
            ROCKSDB_VERIFY_F(jter != obj_info.params.end(), "%s : %s", name, m_ns.data_);
            const auto clazz = jter.value().get_ref<const std::string&>().c_str();
            mg_printf(conn, "<tr><td><a href='/%.*s/%s?html=1'>%s</a></td><td>%s</td></tr>\n",
                      int(urilen), uri, name, name, clazz);
          }
          mg_printf(conn, "</tbody></table></body></html>\n");
        }
      }
      return true;
    }
    const char* name = slash + 1;
    auto iter = m_map->name2p->find(name);
    if (m_map->name2p->end() != iter) {
      auto& p = iter->second;
      if (html) {
        int refresh = JsonSmartInt(query, "refresh", 0);
        if (refresh > 0) {
          mg_printf(conn,
            "<html><title>%s</title>\n"
            "<meta http-equiv='refresh' content='%d'>\n"
            "<body>\n", name, refresh);
        }
        else {
          mg_printf(conn, "<html><title>%s</title><body>\n", name);
        }
        mg_printf(conn, R"(<script>
function SetParam(name, value) {
    const url = new URL(location.href);
    var params = new URLSearchParams(url.search);
    params.set(name, value);
    url.search = params.toString();
    location.href = url.href;
}
</script>)");
        mg_print_cur_time(conn, query, m_repo);
      }
#if defined(NDEBUG)
      try {
#endif
        using namespace std::chrono;
        bool html_time = JsonSmartBool(query, "html_time");
        auto t1 = steady_clock::now();
        std::string str = PluginToString(p, *m_map, query, *m_repo);
        auto t2 = steady_clock::now();
        mg_write(conn, str.data(), str.size());
        if (html_time) {
          double sec = duration_cast<microseconds>(t2-t1).count() / 1e6;
          mg_printf(conn, "<script>"
"document.getElementById('time_stat_line').innerHTML += ', html_time = %.6f sec';"
            "</script>", sec);
        }
#if defined(NDEBUG)
      }
      catch (const Status& es) {
        mg_printf(conn, "Caught Status: %s\n", es.ToString().c_str());
      }
      catch (const std::exception& ex) {
        mg_printf(conn, "Caught std::exception: %s\n", ex.what());
      }
#endif
      if (html)
        mg_printf(conn, "</body></html>\n");
    }
    else if (html) {
      mg_printf(conn, "<html><title>ERROR</title><body>\r\n");
      mg_printf(conn, "<h1>ERROR: not found: %s</h1>\r\n", uri);
      mg_printf(conn, "<h1><a href='/%.*s%s%s'>see all %.*s</a>\r\n",
                int(slash - uri), uri,
                req->query_string ? "?" : "",
                req->query_string ? req->query_string : "",
                int(slash - uri), uri);
      mg_printf(conn, "</body></html>\r\n");
    }
    else {
      mg_printf(conn, R"({status:"NotFound", namespace:"%s", objname:"%s"})",
                m_ns.data_, name);
    }
    return true;
  }
};

template<class Ptr>
RepoHandler<Ptr>*
NewRepoHandler(const char* clazz, SidePluginRepo* repo,
               SidePluginRepo::Impl::ObjMap<Ptr>* map) {
  return new RepoHandler<Ptr>(clazz, repo, map);
}

#define ADD_HANDLER(clazz, varname) do { \
  auto p = NewRepoHandler(#clazz, repo, &repo->m_impl->varname); \
  m_server->addHandler("/" #clazz, *p);  \
  m_server->addHandler("/" #varname, *p);  \
  m_clean.push_back([p](){ delete p; }); \
} while (0)                             \

class JsonCivetServer::Impl {
public:
  std::unique_ptr<CivetServer> m_server;
  std::vector<std::function<void()> > m_clean;

  Impl(const json& conf, SidePluginRepo* repo);
  ~Impl() {
    for (auto& clean: m_clean) {
      clean();
    }
  }
};

JsonCivetServer::Impl::Impl(const json& conf, SidePluginRepo* repo) {
  mg_init_library(0);
  if (!conf.is_object()) {
    THROW_InvalidArgument(
        "conf must be a json object, but is: " + conf.dump());
  }
  std::vector<std::string> options;
  for (const auto& kv : conf.items()) {
    std::string key = kv.key();
    options.push_back(std::move(key));
    const auto& value = kv.value();
    if (value.is_string())
      options.push_back(value.get_ref<const std::string&>());
    else
      options.push_back(value.dump());
  }
  ROCKSDB_VERIFY_AL(options.size(), 2);
  if (SidePluginRepo::DebugLevel() >= 5) {
    for (const auto& val : options) {
      fprintf(stderr, "INFO: JsonCivetServer::Impl::Impl(): len=%02zd: %s\n", val.size(), val.c_str());
    }
  }
  m_server.reset(new CivetServer(options));

  ADD_HANDLER(AnyPlugin, any_plugin);
  ADD_HANDLER(Cache, cache);
  ADD_HANDLER(PersistentCache, persistent_cache);
  ADD_HANDLER(CompactionFilterFactory, compaction_filter_factory);
  ADD_HANDLER(Comparator, comparator);
  ADD_HANDLER(ConcurrentTaskLimiter, compaction_thread_limiter);
  ADD_HANDLER(Env, env);
  ADD_HANDLER(EventListener, event_listener);
  ADD_HANDLER(FileChecksumGenFactory, file_checksum_gen_factory);
  ADD_HANDLER(FileSystem, file_system);
  ADD_HANDLER(FilterPolicy, filter_policy);
  ADD_HANDLER(FlushBlockPolicyFactory, flush_block_policy_factory);
  ADD_HANDLER(Logger, info_log);
  ADD_HANDLER(MemoryAllocator, memory_allocator);
  ADD_HANDLER(MemTableRepFactory, memtable_factory);
  ADD_HANDLER(MergeOperator, merge_operator);
  ADD_HANDLER(RateLimiter, rate_limiter);
  ADD_HANDLER(SstFileManager, sst_file_manager);
  ADD_HANDLER(Statistics, statistics);
  ADD_HANDLER(TableFactory, table_factory);
  ADD_HANDLER(TablePropertiesCollectorFactory, table_properties_collector_factory);
  ADD_HANDLER(TransactionDBMutexFactory, txn_db_mutex_factory);
  ADD_HANDLER(SliceTransform, slice_transform);
  ADD_HANDLER(SstPartitionerFactory, sst_partitioner_factory);
  ADD_HANDLER(CompactionExecutorFactory, compaction_executor_factory);
  ADD_HANDLER(WriteBufferManager, write_buffer_manager);

  ADD_HANDLER(Options, options);
  ADD_HANDLER(DBOptions, db_options);
  ADD_HANDLER(CFOptions, cf_options);

  ADD_HANDLER(CFPropertiesWebView, props);

  //using DataBase = DB_Ptr;
  ADD_HANDLER(DataBase, db);
}

void JsonCivetServer::Init(const json& conf, SidePluginRepo* repo) {
  if (!m_impl)
    m_impl = new Impl(conf, repo);
}
void JsonCivetServer::Close() {
  delete m_impl;
  m_impl = nullptr;
}
JsonCivetServer::JsonCivetServer() {
  m_impl = nullptr;
}
JsonCivetServer::~JsonCivetServer() {
  delete m_impl;
  mg_exit_library();
}

} // ROCKSDB_NAMESPACE
