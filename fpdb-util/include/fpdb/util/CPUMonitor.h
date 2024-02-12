//
// Created by Yifei Yang on 12/19/22.
//

#ifndef FPDB_FPDB_UTIL_INCLUDE_FPDB_UTIL_CPUMONITOR_H
#define FPDB_FPDB_UTIL_INCLUDE_FPDB_UTIL_CPUMONITOR_H

#include <cstdint>

namespace fpdb::util {

/**
 * Monitor CPU usage, the part for linux is from
 * https://stackoverflow.com/questions/63166/how-to-determine-cpu-and-memory-consumption-from-inside-a-process
 */
class CPUMonitor {

public:
  CPUMonitor();

  double getUsage();

private:
#ifdef __linux__
  uint64_t lastTotalUser_;
  uint64_t lastTotalUserLow_;
  uint64_t lastTotalSys_;
  uint64_t lastTotalIdle_;
#endif
};

inline CPUMonitor cpuMonitor;

}


#endif //FPDB_FPDB_UTIL_INCLUDE_FPDB_UTIL_CPUMONITOR_H
