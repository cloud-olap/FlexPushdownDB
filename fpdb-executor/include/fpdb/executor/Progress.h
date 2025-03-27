//
// Created by Yifei Yang on 4/16/24.
//

#ifndef FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PROGRESS_H
#define FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PROGRESS_H

#include <fpdb/executor/physical/PhysicalPlan.h>
#include <thread>

namespace fpdb::executor {

/**
 * Used to display a progress bar of query execution, this class should be called atomically
 */
class Progress {

public:
  Progress(PhysicalPlan* plan);

  void advance(POpType type, bool init);
  void display() ;
  void wait() const;

private:
  static constexpr int FAST_QUERY_CHECK_GAP_MS = 10;
  static constexpr int FAST_QUERY_CHECK_MS = 1000;
  static constexpr int FLUSH_FREQ_MS = 500;
  static void displayBar(double progress);

  void displayImpl();

  PhysicalPlan* plan_;
  int numOpsToTrack_ = 0;
  int numOpsToTrackFinished_ = 0;
  std::shared_ptr<std::thread> displayTh_;
};

}

#endif // FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PROGRESS_H
