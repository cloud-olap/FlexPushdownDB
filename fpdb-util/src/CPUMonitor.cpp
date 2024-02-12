//
// Created by Yifei Yang on 12/19/22.
//

#include <fpdb/util/CPUMonitor.h>
#include <fpdb/util/Util.h>
#include <cstdio>
#include <string>
#include <thread>

namespace fpdb::util {

CPUMonitor::CPUMonitor() {
#ifdef __linux__
  FILE* file = fopen("/proc/stat", "r");
  fscanf(file, "cpu %llu %llu %llu %llu",
         &lastTotalUser_, &lastTotalUserLow_, &lastTotalSys_, &lastTotalIdle_);
  fclose(file);
#elif __APPLE__
  // noop
#else
  throw std::runtime_error("Unsupported OS type for CPU monitor.");
#endif
}

double CPUMonitor::getUsage() {
#ifdef __linux__
  double percent;
  FILE* file;
  uint64_t totalUser, totalUserLow, totalSys, totalIdle, total;

  file = fopen("/proc/stat", "r");
  fscanf(file, "cpu %llu %llu %llu %llu",
         &totalUser, &totalUserLow, &totalSys, &totalIdle);
  fclose(file);

  if (totalUser < lastTotalUser_ || totalUserLow < lastTotalUserLow_ ||
      totalSys < lastTotalSys_ || totalIdle < lastTotalIdle_){
    //Overflow detection. Just skip this value.
    percent = -1.0;
  }
  else{
    total = (totalUser - lastTotalUser_) + (totalUserLow - lastTotalUserLow_) +
            (totalSys - lastTotalSys_);
    percent = total;
    total += (totalIdle - lastTotalIdle_);
    percent /= total;
    percent *= 100;
  }

  lastTotalUser_ = totalUser;
  lastTotalUserLow_ = totalUserLow;
  lastTotalSys_ = totalSys;
  lastTotalIdle_ = totalIdle;

  return percent;

#elif __APPLE__
  // FIXME: this is not the current usage, but sum of all average usage of all processes
  //  from the time when the process is started
  auto expCpuUsageOverOneCore = execCmd("ps -A -o %cpu | awk '{s+=$1} END {print s}'");
  if (!expCpuUsageOverOneCore.has_value()) {
    throw runtime_error(expCpuUsageOverOneCore.error());
  }
  return stod(*expCpuUsageOverOneCore) / ((double) std::thread::hardware_concurrency());

#else
  throw std::runtime_error("Unsupported OS type for CPU monitor.");
#endif
}

}
