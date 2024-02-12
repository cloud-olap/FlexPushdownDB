//
// Created by Yifei Yang on 10/31/22.
//

#ifndef FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_COLLADAPTPUSHDOWNMETRICSEXECUTION_H
#define FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_COLLADAPTPUSHDOWNMETRICSEXECUTION_H

#include <fpdb/executor/Execution.h>
#include <fpdb/catalogue/obj-store/fpdb-store/FPDBStoreConnector.h>

namespace fpdb::executor {

/**
 * Type of execution that used to collect metrics for adaptive pushdown
 */
class CollAdaptPushdownMetricsExecution: public Execution {

public:
  CollAdaptPushdownMetricsExecution(
          long queryId,
          const shared_ptr<::caf::actor_system> &actorSystem,
          const vector<::caf::node_id> &nodes,
          const ::caf::actor &localSegmentCacheActor,
          const vector<::caf::actor> &remoteSegmentCacheActors,
          const shared_ptr<PhysicalPlan> &physicalPlan,
          bool isDistributed,
          const std::shared_ptr<fpdb::catalogue::obj_store::FPDBStoreConnector> &fpdbStoreConnector);
  ~CollAdaptPushdownMetricsExecution() override = default;

  shared_ptr<TupleSet> execute() override;

private:
  void preExecute() override;
  void join() override;

  bool useDetached(const shared_ptr<PhysicalOp> &op) override;

  void sendAdaptPushdownMetricsToStore();
  void addAdaptPushdownMetrics(const std::string &key, int64_t execTime);

  std::unordered_map<std::string, int64_t> adaptPushdownMetrics_;
  std::shared_ptr<fpdb::catalogue::obj_store::FPDBStoreConnector> fpdbStoreConnector_;
};

}


#endif //FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_COLLADAPTPUSHDOWNMETRICSEXECUTION_H
