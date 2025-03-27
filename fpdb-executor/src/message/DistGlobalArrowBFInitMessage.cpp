//
// Created by Yifei Yang on 4/17/24.
//

#include <fpdb/executor/message/DistGlobalArrowBFInitMessage.h>

namespace fpdb::executor::message {

DistGlobalArrowBFInitMessage::DistGlobalArrowBFInitMessage(
        int64_t numRows,
        const std::shared_ptr<arrow::compute::BloomFilterMasks> &masks,
        const std::string &sender):
  Message(DIST_GLOBAL_BF_INIT, sender),
  numRows_(numRows),
  masks_(masks) {}

std::string DistGlobalArrowBFInitMessage::getTypeString() const {
  return "DistGlobalArrowBFInitMessage";
}

int64_t DistGlobalArrowBFInitMessage::getNumRows() const {
  return numRows_;
}

const std::shared_ptr<arrow::compute::BloomFilterMasks> &DistGlobalArrowBFInitMessage::getMasks() const {
  return masks_;
}

}
