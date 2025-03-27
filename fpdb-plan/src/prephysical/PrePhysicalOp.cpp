//
// Created by Yifei Yang on 10/31/21.
//

#include <fpdb/plan/prephysical/PrePhysicalOp.h>

namespace fpdb::plan::prephysical {

PrePhysicalOp::PrePhysicalOp(uint id, PrePOpType type, double rowCount) :
  id_(id),
  type_(type),
  rowCount_(rowCount) {}

bool PrePhysicalOp::equals(const std::shared_ptr<PrePhysicalOp> &p1,
                           const std::shared_ptr<PrePhysicalOp> &p2) {
  if (p1 == nullptr && p2 == nullptr) {
    return true;
  } else if (p1 != nullptr && p2 != nullptr) {
    return p1->equalTo(p2);
  } else {
    return false;
  }
}

bool PrePhysicalOp::equals(const std::vector<std::shared_ptr<PrePhysicalOp>> &p1,
                           const std::vector<std::shared_ptr<PrePhysicalOp>> &p2) {
  if (p1.size() != p2.size()) {
    return false;
  }
  for (uint i = 0; i < p1.size(); ++i) {
    if (!equals(p1[i], p2[i])) {
      return false;
    }
  }
  return true;
}

uint PrePhysicalOp::getId() const {
  return id_;
}

PrePOpType PrePhysicalOp::getType() const {
  return type_;
}

const vector<shared_ptr<PrePhysicalOp>> &PrePhysicalOp::getProducers() const {
  return producers_;
}

const set<string> &PrePhysicalOp::getProjectColumnNames() const {
  return projectColumnNames_;
}

double PrePhysicalOp::getRowCount() const {
  return rowCount_;
}

void PrePhysicalOp::setProducers(const vector<shared_ptr<PrePhysicalOp>> &producers) {
  producers_ = producers;
}

void PrePhysicalOp::setProjectColumnNames(const set<string> &projectColumnNames) {
  projectColumnNames_ = projectColumnNames;
}

void PrePhysicalOp::setRowCount(double rowCount) {
  rowCount_ = rowCount;
}

}
