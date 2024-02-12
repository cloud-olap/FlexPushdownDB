//
// Created by Yifei Yang on 4/22/22.
//

#ifndef FPDB_FPDB_TUPLE_INCLUDE_FPDB_TUPLE_ARROW_EXEC_DUMMYNODE_H
#define FPDB_FPDB_TUPLE_INCLUDE_FPDB_TUPLE_ARROW_EXEC_DUMMYNODE_H

#include <arrow/compute/exec/exec_plan.h>
#include <arrow/compute/exec/options.h>

namespace arrow::compute {

/**
 * A dummy node for arrow execution engine which just forwards received batches to its consumer
 */
class DummyNode: public ExecNode {

public:
  DummyNode(ExecPlan* plan, const std::shared_ptr<Schema> &outputSchema);

  static Result<ExecNode*> Make(ExecPlan* plan, const std::shared_ptr<Schema> &outputSchema);

  const char* kind_name() const override;

  void InputReceived(ExecNode*, ExecBatch batch) override;
  void ErrorReceived(ExecNode*, Status) override;
  void InputFinished(ExecNode*, int total_batches) override;

  Status StartProducing() override;
  void PauseProducing(ExecNode*) override;
  void ResumeProducing(ExecNode*) override;
  void StopProducing(ExecNode*) override;
  void StopProducing() override;

  Future<> finished() override;

private:
  Future<> finished_ = Future<>::MakeFinished();

};

}


#endif //FPDB_FPDB_TUPLE_INCLUDE_FPDB_TUPLE_ARROW_EXEC_DUMMYNODE_H
