//
// Created by Yifei Yang on 4/16/24.
//

#include <fpdb/executor/Progress.h>
#include <chrono>

namespace fpdb::executor {

Progress::Progress(PhysicalPlan* plan): plan_(plan) {}

void Progress::advance(POpType type, bool init) {
  switch (type) {
    // exclude almost no-time ops
    case POpType::CACHE_LOAD:
    case POpType::MERGE:
    case POpType::ADAPT_SINK: {
      return;
    }
    default: {
      init ? ++numOpsToTrack_ : ++numOpsToTrackFinished_;
    }
  }
}

void Progress::display() {
  displayTh_ = std::make_shared<std::thread>(&Progress::displayImpl, this);
}

void Progress::displayBar(double progress) {
  int barWidth = 70;
  std::cout << "Progress [";
  int pos = barWidth * progress;
  for (int i = 0; i < barWidth; ++i) {
    if (i < pos) std::cout << "=";
    else if (i == pos) std::cout << ">";
    else std::cout << " ";
  }
  std::cout << "] " << int(progress * 100.0) << " %\r";
  std::cout.flush();
}

void Progress::displayImpl() {
  // init
  for (const auto &op: plan_->getPhysicalOps()) {
    advance(op.second->getType(), true);
  }

  // check if we need to display
  if (numOpsToTrack_ == 0) {
    return;
  }

  // some queries may finish super fast then do not show the bar
  double progress;
  for (int i = 0; i < FAST_QUERY_CHECK_MS / FAST_QUERY_CHECK_GAP_MS; ++i) {
    progress = 1.0 * numOpsToTrackFinished_ / numOpsToTrack_;
    if (progress >= 1.0) {
      return;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(FAST_QUERY_CHECK_GAP_MS));
  }

  // display the progress bar
  progress = 1.0 * numOpsToTrackFinished_ / numOpsToTrack_;
  while (progress < 1.0) {
    // update bar
    displayBar(progress);
    // update progress
    std::this_thread::sleep_for(std::chrono::milliseconds(FLUSH_FREQ_MS));
    progress = 1.0 * numOpsToTrackFinished_ / numOpsToTrack_;
  }
  displayBar(1.0);
}

void Progress::wait() const {
  displayTh_->join();
}

}
