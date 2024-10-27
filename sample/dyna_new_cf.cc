#include <topling/side_plugin_repo.h>
#include <rocksdb/db.h>
#include <thread>

void usage(int line, const char* prog) {
  fprintf(stderr, "%d: usage: %s conf-file\n", line, prog);
  exit(1);
}

int main(int argc, char* argv[]) {
  if (argc < 2) {
    usage(__LINE__, argv[0]);
  }
  using namespace rocksdb;
  SidePluginRepo repo;
  Status s = repo.ImportAutoFile(argv[1]);
  if (!s.ok()) {
    fprintf(stderr, "ERROR: repo.ImportAutoFile(%s) = %s\n", s.ToString().c_str(), strerror(errno));
    return 2;
  }
  DB_MultiCF* mcf = nullptr;
  s = repo.OpenDB(&mcf);
  if (!s.ok()) {
    fprintf(stderr, "ERROR: repo.OpenDB(DB*) = %s\n", s.ToString().c_str());
    return 2;
  }
  TERARK_SCOPE_EXIT(repo.CloseAllDB());
  s = repo.StartHttpServer();
  if (!s.ok()) {
    fprintf(stderr, "ERROR: repo.StartHttpServer() = %s\n", s.ToString().c_str());
    return 2;
  }
  fprintf(stderr, "now visit the web(defined in json/yaml conf file)\n");
  fprintf(stderr, "press enter to exit\n");
  bool stop = false;
  std::thread thr([&]{
    size_t i = 0;
    std::vector<std::string> cfjs_vec = {
      R"("${default}")", "'default'", "default",
      R"({"template": "default", "table_factory": "bb"})",
      R"({"template": "${default}", "table_factory": "zip"})",
    };
    while (!stop) {
      i++;
      auto& cfjs = cfjs_vec[i % cfjs_vec.size()];
      rocksdb::ColumnFamilyHandle* cfh = nullptr;
      auto s = mcf->CreateColumnFamily("cf-" + std::to_string(i), cfjs, &cfh);
      if (s.ok()) {
        fprintf(stderr, "CreateColumnFamily(cf-%zd) success\n", i);
      } else {
        fprintf(stderr, "CreateColumnFamily(cf-%zd) fail: %s\n", i, s.ToString().c_str());
      }
      usleep(2000000);
    }
  });
  getchar(); // wait for enter
  stop = true;
  thr.join();
  return 0;
}
