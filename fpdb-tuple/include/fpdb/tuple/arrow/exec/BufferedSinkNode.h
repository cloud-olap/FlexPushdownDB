//
// Created by Yifei Yang on 4/27/22.
//

#ifndef FPDB_FPDB_TUPLE_INCLUDE_FPDB_TUPLE_ARROW_EXEC_BUFFEREDSINKNODE_H
#define FPDB_FPDB_TUPLE_INCLUDE_FPDB_TUPLE_ARROW_EXEC_BUFFEREDSINKNODE_H

#include <fpdb/tuple/TupleSet.h>
#include <arrow/compute/exec/exec_plan.h>
#include <arrow/compute/exec/options.h>
#include <tl/expected.hpp>

using namespace fpdb::tuple;

namespace arrow::compute {

/**
 * A sink node which pops buffered batches when exceeding a specified size
 */
class BufferedSinkNode: public ExecNode {

public:
  using BufferOutputCallback =
          std::function<tl::expected<void, std::string>(const std::shared_ptr<TupleSet> &tupleSet)>;

  BufferedSinkNode(ExecPlan* plan, std::vector<ExecNode*> inputs,
                   std::shared_ptr<arrow::Schema> outputSchema, int64_t bufferCapacity,
                   BufferOutputCallback bufferOutputCallback);

  static Result<ExecNode*> Make(ExecPlan* plan, std::vector<ExecNode*> inputs,
                                std::shared_ptr<arrow::Schema> outputSchema, int64_t bufferCapacity,
                                BufferOutputCallback bufferOutputCallback);

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
  Status popBuffer();
  void clearBuffer();

  Future<> finished_ = Future<>::MakeFinished();
  int64_t bufferCapacity_;
  BufferOutputCallback bufferOutputCallback_;

  arrow::RecordBatchVector buffer_;
  int64_t bufferSize_ = 0;

};

}


#endif //FPDB_FPDB_TUPLE_INCLUDE_FPDB_TUPLE_ARROW_EXEC_BUFFEREDSINKNODE_H
