#include <topling/side_plugin_repo.h>
#include <rocksdb/db.h>

void usage(int line, const char* prog) {
  fprintf(stderr, "%d: usage: %s conf-file [get key] | [set key value]\n", line, prog);
  exit(1);
}

int main(int argc, char* argv[]) {
  if (argc < 4) {
    usage(__LINE__, argv[0]);
  }
  if (strcasecmp(argv[2], "get") != 0 && strcasecmp(argv[2], "set") != 0) {
    usage(__LINE__, argv[0]);
  }
  if (strcasecmp(argv[2], "set") == 0 && argc < 5) {
    usage(__LINE__, argv[0]);
  }
  using namespace rocksdb;
  SidePluginRepo repo;
  Status s = repo.ImportAutoFile(argv[1]);
  if (!s.ok()) {
    fprintf(stderr, "ERROR: repo.ImportAutoFile(%s) = %s\n", s.ToString().c_str());
    return 2;
  }
  DB* db = nullptr;
  s = repo.OpenDB(&db);
  if (!s.ok()) {
    fprintf(stderr, "ERROR: repo.OpenDB(DB*) = %s\n", s.ToString().c_str());
    return 2;
  }
  s = repo.StartHttpServer();
  if (!s.ok()) {
    fprintf(stderr, "ERROR: repo.StartHttpServer() = %s\n", s.ToString().c_str());
    return 2;
  }
  if (strcasecmp(argv[2], "set") == 0) {
    s = db->Put(WriteOptions(), argv[3], argv[4]);
    if (!s.ok()) {
      fprintf(stderr, "ERROR: db->Put(%s, %s) = %s\n", argv[3], argv[4], s.ToString().c_str());
      return 2;
    }
  }
  else if (strcasecmp(argv[2], "get") == 0) {
    std::string val;
    s = db->Get(ReadOptions(), argv[3], &val);
    if (!s.ok()) {
      fprintf(stderr, "ERROR: db->Get(%s) = %s\n", argv[3], s.ToString().c_str());
      return 2;
    }
    fprintf(stdout, "%s\n", val.c_str());
  }
  fprintf(stderr, "now visit the web(defined in json/yaml conf file)\n");
  fprintf(stderr, "press enter to exit\n");
  getchar(); // wait for enter
  repo.CloseAllDB();
  return 0;
}
