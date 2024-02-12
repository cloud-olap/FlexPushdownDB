//
// Created by Yifei Yang on 4/27/22.
//

#include <fpdb/tuple/arrow/exec/DummyNode.h>

namespace arrow::compute {

DummyNode::DummyNode(ExecPlan* plan, const std::shared_ptr<Schema> &outputSchema):
  ExecNode(plan, {}, {}, outputSchema, 1) {}

Result<ExecNode*> DummyNode::Make(ExecPlan* plan, const std::shared_ptr<Schema> &outputSchema) {
  return plan->EmplaceNode<DummyNode>(plan, outputSchema);
}

const char* DummyNode::kind_name() const {
  return "DummyNode";
}

void DummyNode::InputReceived(ExecNode*, ExecBatch batch) {
  outputs_[0]->InputReceived(this, std::move(batch));
}

void DummyNode::ErrorReceived(ExecNode*, Status) {
  // noop
}

void DummyNode::InputFinished(ExecNode*, int total_batches) {
  outputs_[0]->InputFinished(this, total_batches);
}

Status DummyNode::StartProducing() {
  finished_ = Future<>::Make();
  return Status::OK();
}

void DummyNode::PauseProducing(ExecNode*) {
  // noop
}

void DummyNode::ResumeProducing(ExecNode*) {
  // noop
}

void DummyNode::StopProducing(ExecNode*) {
  StopProducing();
}

void DummyNode::StopProducing() {
  finished_.MarkFinished();
}

Future<> DummyNode::finished() {
  return finished_;
}

}
