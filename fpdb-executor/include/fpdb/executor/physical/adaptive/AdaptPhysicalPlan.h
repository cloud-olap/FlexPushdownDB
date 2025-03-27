//
// Created by Yifei Yang on 4/5/24.
//

#ifndef FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_ADAPTIVE_ADAPTPHYSICALPLAN_H
#define FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_ADAPTIVE_ADAPTPHYSICALPLAN_H

#include <fpdb/executor/physical/PhysicalPlan.h>
#include <unordered_set>

namespace fpdb::executor::physical {

/**
 * A stage of the entire regular query plan in adaptive exec
 */
class AdaptPhysicalPlan: public PhysicalPlan {

public:
  AdaptPhysicalPlan(const std::unordered_map<std::string, std::shared_ptr<PhysicalOp>> &physicalOps,
                    const std::unordered_set<std::string> &sinkToProduce,
                    const std::unordered_map<std::string, bool> &sinkToConsume,
                    bool consumeAllSink,
                    const std::string &rootPOpName = "" /*a stage may not have a root*/);

  size_t numSinkToProduce() const;
  size_t numSinkToConsume() const;
  size_t numSinkToComplete() const;   // among "sinkToConsume_", how many will complete in this stage (i.e., not preserved)

  bool isSinkToProduce(const std::string &op) const;
  bool isSinkToConsume(const std::string &op) const;
  bool isSinkToConsume(const std::string &op, bool* preserve) const;
  bool consumeAllSink() const;

private:
  std::unordered_set<std::string> sinkToProduce_;
  std::unordered_map<std::string, bool> sinkToConsume_;   // a map of <sink, whether to preserve after this consumption>
  bool consumeAllSink_;   // consume all unfinished sink, if true then "sinkToConsume_" should not be used
};

}

#endif //FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_ADAPTIVE_ADAPTPHYSICALPLAN_H
