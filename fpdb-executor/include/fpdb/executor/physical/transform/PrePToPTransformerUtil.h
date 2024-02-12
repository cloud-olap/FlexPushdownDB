//
// Created by Yifei Yang on 2/22/22.
//

#ifndef FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_TRANSFORM_PREPTOPTRANSFORMERUTIL_H
#define FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_TRANSFORM_PREPTOPTRANSFORMERUTIL_H

#include <fpdb/executor/physical/PhysicalOp.h>
#include <fpdb/executor/physical/PhysicalPlan.h>
#include <fpdb/executor/physical/aggregate/function/AggregateFunction.h>
#include <fpdb/plan/prephysical/AggregatePrePFunction.h>
#include <unordered_map>

using namespace fpdb::plan::prephysical;

namespace fpdb::executor::physical {

class PrePToPTransformerUtil {

public:
  /**
   * Connect producers and consumers
   * @param producers
   * @param consumers
   */
  static void connectOneToOne(vector<shared_ptr<PhysicalOp>> &producers,
                              vector<shared_ptr<PhysicalOp>> &consumers);
  static void connectOneToOne(shared_ptr<PhysicalOp> &producer,
                              shared_ptr<PhysicalOp> &consumer);
  static void connectManyToOne(vector<shared_ptr<PhysicalOp>> &producers,
                               shared_ptr<PhysicalOp> &consumer);
  static void connectOneToMany(shared_ptr<PhysicalOp> &producer,
                               vector<shared_ptr<PhysicalOp>> &consumers);
  static void connectManyToMany(vector<shared_ptr<PhysicalOp>> &producers,
                                vector<shared_ptr<PhysicalOp>> &consumers);

  /**
   * Transform aggregate and aggregate reduce function
   * @param outputColumnName
   * @param prePFunction
   * @param hasReduceOp whether there is a reduce op as the consumer for all parallel ops
   * @return
   */
  static vector<shared_ptr<aggregate::AggregateFunction>>
  transformAggFunction(const string &outputColumnName,
                       const shared_ptr<AggregatePrePFunction> &prePFunction,
                       bool hasReduceOp);
  static shared_ptr<aggregate::AggregateFunction>
  transformAggReduceFunction(const string &outputColumnName,
                             const shared_ptr<AggregatePrePFunction> &prePFunction);

  /**
   * Make a physical plan from the root physical operator
   * @param rootOp
   * @return
   */
  static shared_ptr<PhysicalPlan> rootOpToPlan(const shared_ptr<PhysicalOp> &rootOp,
                                               const unordered_map<string, shared_ptr<PhysicalOp>> &opMap);

  /**
   * Make a physical plan from the root physical operator, also extract the host that plan is sent to
   * The plan should be a sub-plan of FPDBStoreSuperPOp
   * @param rootOp
   * @param opMap
   * @param objectToHost
   * @return
   */
  static pair<shared_ptr<PhysicalPlan>, std::string> rootOpToPlanAndHost(
          const shared_ptr<PhysicalOp> &rootOp,
          const unordered_map<string, shared_ptr<PhysicalOp>> &opMap,
          const unordered_map<std::string, std::string> &objectToHost);

  /**
   * Update the map of <op, store node id> for a serial order of ops from root
   * Ops should contain at least one FPDBStoreFileScanPOp
   * @param opToStoreNode
   * @param rootOp
   * @param opMap
   * @param objectToHost
   * @param hostToId
   */
  static void updateOpToStoreNode(unordered_map<string, int> &opToStoreNode,
                                  const shared_ptr<PhysicalOp> &rootOp,
                                  const unordered_map<string, shared_ptr<PhysicalOp>> &opMap,
                                  const unordered_map<std::string, std::string> &objectToHost,
                                  const unordered_map<std::string, int> &hostToId);

  /**
   * Add physical operators to existing ones
   * @param newOps
   * @param ops
   */
  static void addPhysicalOps(const vector<shared_ptr<PhysicalOp>> &newOps,
                             unordered_map<string, shared_ptr<PhysicalOp>> &ops);

  /**
   * Get a map of <host, num ops>
   * @param fpdbStoreSuperPOps
   * @return
   */
  static unordered_map<string, int> getHostToNumOps(const vector<shared_ptr<PhysicalOp>> &fpdbStoreSuperPOps);

};

}


#endif //FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_TRANSFORM_PREPTOPTRANSFORMERUTIL_H
