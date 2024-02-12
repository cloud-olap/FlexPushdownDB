//
// Created by Yifei Yang on 4/27/22.
//

#include <fpdb/tuple/arrow/exec/BufferedSinkNode.h>

namespace arrow::compute {

BufferedSinkNode::BufferedSinkNode(ExecPlan* plan, std::vector<ExecNode*> inputs,
                                   std::shared_ptr<arrow::Schema> outputSchema, int64_t bufferCapacity,
                                   BufferOutputCallback bufferOutputCallback):
  ExecNode(plan, std::move(inputs), {"collected"}, std::move(outputSchema), 0),
  bufferCapacity_(bufferCapacity),
  bufferOutputCallback_(std::move(bufferOutputCallback)) {}

Result<ExecNode*> BufferedSinkNode::Make(ExecPlan* plan, std::vector<ExecNode*> inputs,
                                         std::shared_ptr<arrow::Schema> outputSchema, int64_t bufferCapacity,
                                         BufferOutputCallback bufferOutputCallback) {
  return plan->EmplaceNode<BufferedSinkNode>(plan, std::move(inputs), std::move(outputSchema),
                                             bufferCapacity, std::move(bufferOutputCallback));
}

const char* BufferedSinkNode::kind_name() const {
  return "BufferedSinkNode";
}

Status BufferedSinkNode::StartProducing() {
  finished_ = Future<>::Make();
  return Status::OK();
}

void BufferedSinkNode::InputReceived(ExecNode*, ExecBatch batch) {
  auto recordBatch = batch.ToRecordBatch(output_schema_);
  ErrorIfNotOk(recordBatch.status());
  buffer_.emplace_back(*recordBatch);
  bufferSize_ += (*recordBatch)->num_rows();
  if (bufferSize_ >= bufferCapacity_) {
    ErrorIfNotOk(popBuffer());
  }
}

void BufferedSinkNode::ErrorReceived(ExecNode*, Status) {
  // noop
}

void BufferedSinkNode::InputFinished(ExecNode*, int) {
  if (bufferSize_ > 0) {
    ErrorIfNotOk(popBuffer());
  }
}

void BufferedSinkNode::PauseProducing(ExecNode*) {
  // noop
}

void BufferedSinkNode::ResumeProducing(ExecNode*) {
  // noop
}

void BufferedSinkNode::StopProducing(ExecNode*) {
  StopProducing();
}

void BufferedSinkNode::StopProducing() {
  finished_.MarkFinished();
}

Future<> BufferedSinkNode::finished() {
  return finished_;
}

Status BufferedSinkNode::popBuffer() {
  ARROW_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Table> table, arrow::Table::FromRecordBatches(buffer_));
  auto bufferRes = bufferOutputCallback_(TupleSet::make(table));
  if (!bufferRes.has_value()) {
    return Status::ExecutionError(bufferRes.error());
  }
  clearBuffer();
  return Status::OK();
}

void BufferedSinkNode::clearBuffer() {
  buffer_.clear();
  bufferSize_ = 0;
}
  
}
