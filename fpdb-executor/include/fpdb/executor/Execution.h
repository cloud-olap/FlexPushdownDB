//
// Created by Yifei Yang on 11/23/21.
//

#ifndef FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_EXECUTION_H
#define FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_EXECUTION_H

#include <fpdb/executor/physical/PhysicalOp.h>
#include <fpdb/executor/physical/PhysicalPlan.h>
#include <fpdb/executor/physical/POpDirectory.h>
#include <fpdb/executor/physical/adaptive/AdaptPhysicalPlan.h>
#include <fpdb/executor/physical/collate/CollatePOp.h>
#include <fpdb/executor/physical/collate/CollatePOp2.h>
#include <fpdb/executor/physical/s3/S3SelectScanAbstractPOp.h>
#include <fpdb/executor/cache/TableCache.h>
#include <fpdb/executor/metrics/DebugMetrics.h>
#include <fpdb/tuple/TupleSet.h>
#include <caf/all.hpp>
#include <memory>

using namespace fpdb::executor::physical;
using namespace std;

namespace fpdb::executor {

inline constexpr const char *ExecutionRootActorName = "execution_root";

/**
 * Execution of a single query
 */
class Execution {

public:
  Execution(long queryId,
            const shared_ptr<::caf::actor_system> &actorSystem,
            const vector<::caf::node_id> &nodes,
            const ::caf::actor &localSegmentCacheActor,
            const vector<::caf::actor> &remoteSegmentCacheActors,
            const shared_ptr<PhysicalPlan> &physicalPlan,
            bool isDistributed,
            void* Executor);
  virtual ~Execution();

  void enableAdaptExec(const shared_ptr<AdaptPhysicalPlan> &adaptPhysicalPlan);
  virtual void execute();

  shared_ptr<TupleSet> getQueryResult() const;
  long getQueryId() const;
  long getElapsedTime();
  shared_ptr<PhysicalOp> getPhysicalOp(const std::string &name);
  physical::s3::S3SelectScanStats getAggregateS3SelectScanStats();
  std::tuple<size_t, size_t, size_t> getFilterTimeNSInputOutputBytes();
  string showRegularMetrics();

  void write_graph(const string &file);

#if SHOW_DEBUG_METRICS == true
  string showDebugMetrics();    // should be called after "showMetrics()"
  const metrics::DebugMetrics &getDebugMetrics() const;
#endif

protected:
  virtual void preExecute();
  void boot();
  void start();
  virtual void join();
  void clear(bool forAdapt);
  void close();

  void spawn(POpDirectoryEntry &opEntry);
  ::caf::actor localSpawn(const shared_ptr<PhysicalOp> &op);
  ::caf::actor remoteSpawn(const shared_ptr<PhysicalOp> &op, int nodeId);
  virtual bool useDetached(const shared_ptr<PhysicalOp> &op);

  void sendConnect(const POpDirectoryEntry &opEntry, const ::caf::scoped_actor &sender);
  void sendStart(const POpDirectoryEntry &opEntry, const ::caf::scoped_actor &sender);

  void fetchOpExecTimes();

  long queryId_;
  shared_ptr<::caf::actor_system> actorSystem_;
  vector<::caf::node_id> nodes_;
  shared_ptr<::caf::scoped_actor> rootActor_;
  ::caf::actor localSegmentCacheActor_;             // used in single-node execution
  vector<::caf::actor> remoteSegmentCacheActors_;   // used in distributed execution
  shared_ptr<PhysicalPlan> physicalPlan_;
  bool isDistributed_;
  void* executor_;

  POpDirectory opDirectory_;
  shared_ptr<physical::collate::CollatePOp> collateOp_;

  // for adaptive exec
  struct {
    bool isAdapt_ = false;
    shared_ptr<AdaptPhysicalPlan> adaptPhysicalPlan_ = nullptr;
  } adaptSt_;

  // for execution time
  chrono::steady_clock::time_point startTime_;
  chrono::steady_clock::time_point stopTime_;

  // recorded op execution time, saved to avoid duplicate fetch
  static constexpr std::string_view NetworkTimeKey = "Network";
  bool isOpTimeFetched_ = false;
  long totalOpTime_ = 0;      // this is the sum of all op exec times including network time, not query exec time
  std::map<std::string, long> opTimes_, networkTimes_;
  std::map<std::string, long long> opTypeTimes_;

  // metrics
#if SHOW_DEBUG_METRICS == true
  metrics::DebugMetrics debugMetrics_;
  long totalPredTransOpTime_ = 0;
  long totalJoinOpTime_ = 0;
  int64_t totalPredTransInterComputeBytes_ = 0;
  int64_t totalJoinInterComputeBytes_ = 0;
#endif

};

}


#endif //FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_EXECUTION_H
