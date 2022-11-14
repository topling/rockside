
#include "topling/side_plugin_factory.h"
#include <terark/hash_strmap.hpp>
#include <terark/util/linebuf.hpp>
#include <terark/util/process.hpp>

namespace ROCKSDB_NAMESPACE {
using namespace terark;

static const hash_strmap<> cpu_basic{
  "Model name",
  "Architecture",
  "CPU(s)",
  "Socket(s)",
  "Thread(s) per core",
  "Core(s) per socket",
  "Virtualization",
};
static const hash_strmap<> cpu_numa{
  "NUMA node(s)",
  "NUMA node0 CPU(s)",
  "NUMA node1 CPU(s)",
  "NUMA node2 CPU(s)",
  "NUMA node3 CPU(s)",
  "NUMA node4 CPU(s)",
  "NUMA node5 CPU(s)",
  "NUMA node6 CPU(s)",
  "NUMA node7 CPU(s)",
  "NUMA node8 CPU(s)",
  "NUMA node9 CPU(s)",
  "NUMA node10 CPU(s)",
  "NUMA node11 CPU(s)",
  "NUMA node12 CPU(s)",
  "NUMA node13 CPU(s)",
  "NUMA node14 CPU(s)",
  "NUMA node15 CPU(s)",
};
static const hash_strmap<> cpu_speed{
  "CPU MHz",
  "CPU min MHz",
  "CPU max MHz",
  "BogoMIPS",
};
static const hash_strmap<> cpu_cache{
  "L1i cache",
  "L1d cache",
  "L2 cache",
  "L3 cache",
  "L4 cache", // some cpu has L4
};
static const hash_strmap<> cpu_flags{"Flags"};

class SysInfoShower : public AnyPlugin {
public:
  const char* Name() const final { return "SysInfoShower"; }
  void Update(const json&, const json&, const SidePluginRepo &) {}
  std::string ToString(const json& dump_options, const SidePluginRepo &) const {
    bool html = JsonSmartBool(dump_options, "html", true);
    json js;
    {
      fstring cmd = html ? "free -wh" : "free -wb";
      ProcPipeStream file(cmd, "r");
      LineBuf buf(file);
      valvec<fstring> lines(3, valvec_reserve());
      if (fstring(buf).split('\n', &lines, 3) != 3) {
        THROW_Corruption("cmd " + cmd + " output error: \n" + buf);
      }
      valvec<fstring> head(8, valvec_reserve());
      valvec<fstring> dram(8, valvec_reserve());
      valvec<fstring> swap(8, valvec_reserve());
      if (lines[0].split(' ', &head) != 7) {
        THROW_Corruption("cmd " + cmd + " output error on 1st line:\n" + buf);
      }
      if (lines[1].split(' ', &dram) != 8) {
        THROW_Corruption("cmd " + cmd + " output error on 2nd line:\n" + buf);
      }
      lines[2].split(' ', &swap);
      json jmem, jdram, jswap;
      jdram["-"] = "DRAM";
      jswap["-"] = "SWAP";
      for (size_t i = 0; i < head.size(); ++i) {
        if (i+1 < dram.size())
          jdram[head[i].str()] = dram[i+1].str();
        else if (html)
          jdram[head[i].str()] = "<em>N/A</em>";
      }
      for (size_t i = 0; i < head.size(); ++i) {
        if (i+1 < swap.size())
          jswap[head[i].str()] = swap[i+1].str();
        else if (html)
          jswap[head[i].str()] = "<em>N/A</em>";
      }
      jmem.push_back(std::move(jdram));
      jmem.push_back(std::move(jswap));
      if (html) {
        json& jtabcols = jmem[0]["<htmltab:col>"];
        jtabcols.push_back("-");
        for (size_t i = 0; i < head.size(); ++i) {
          jtabcols.push_back(head[i].str());
        }
      }
      js["Memory"] = std::move(jmem);
    }

    {
      fstring cmd = "LC_ALL=C lscpu -J";
      ProcPipeStream file(cmd, "r");
      LineBuf buf(file);
      json js_lscpu_output;
      try {
        js_lscpu_output = json::parse(buf.begin(), buf.end());
      }
      catch (const std::exception& ex) {
        THROW_Corruption("cmd " + cmd + " output json error: \n" + buf);
      }
      if (!js_lscpu_output.contains("lscpu")) {
        THROW_Corruption("cmd " + cmd +
          " output json error(missing sub obj 'lscpu'): \n" + buf);
      }
      hash_strmap<const std::string*> lscpu_map;
      for (auto& kv: js_lscpu_output["lscpu"]) {
        auto name1 = kv["field"].get_ref<const std::string&>();
        if (name1.empty()) continue;
        auto name2 = fstring(name1).notail(1); // remove trailing ':'
        TERARK_VERIFY(!name1.empty());
        lscpu_map[name2] = &kv["data"].get_ref<const std::string&>();
      }
      //TERARK_DIE("lscpu_map OK");
      auto get_grp = [&](const hash_strmap<>& group) {
        json res;
        for (size_t i = 0; i < group.end_i(); ++i) {
          fstring key = group.key(i);
          size_t idx = lscpu_map.find_i(key);
          if (lscpu_map.end_i() != idx)
            res[key.str()] = *lscpu_map.val(idx);
         #if 0
          else
            THROW_Corruption("cmd " + cmd +
              " output json error: \nNot Found " + key + "\n" + buf);
         #endif
        }
        return res;
      };
      auto& js_cpu = js["CPU"];
      js_cpu["Basic"] = get_grp(cpu_basic);
      js_cpu["NUMA" ] = get_grp(cpu_numa);
      js_cpu["Speed"] = get_grp(cpu_speed);
      js_cpu["Cache"] = get_grp(cpu_cache);
      js_cpu["Flags"] = get_grp(cpu_flags)["Flags"];

      json jthermal;
      for (int zone = 0; ; zone++) {
        auto strzone = std::to_string(zone);
        auto fname = "/sys/class/thermal/thermal_zone" + strzone + "/temp";
        FILE* fp = fopen(fname.c_str(), "r");
        if (!fp) break;
        LineBuf line(fp);
        line.chomp();
        fclose(fp);
        double val = strtod(line.p, NULL);
        jthermal["CPU" + strzone] = val / 1000.0; // 摄氏度
      }
      js_cpu["Thermal"] = jthermal;
    }

    return JsonToString(js, dump_options);
  }
};

ROCKSDB_REG_Plugin(SysInfoShower, AnyPlugin);
ROCKSDB_REG_AnyPluginManip("SysInfoShower");

} // namespace topling
