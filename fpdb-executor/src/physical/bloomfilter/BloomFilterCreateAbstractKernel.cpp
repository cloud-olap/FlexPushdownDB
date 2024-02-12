//
// Created by Yifei Yang on 11/23/22.
//

#include <fpdb/executor/physical/bloomfilter/BloomFilterCreateAbstractKernel.h>

namespace fpdb::executor::physical::bloomfilter {

BloomFilterCreateAbstractKernel::BloomFilterCreateAbstractKernel(BloomFilterCreateKernelType type,
                                                                 const std::vector<std::string> &columnNames):
  type_(type),
  columnNames_(columnNames) {}

BloomFilterCreateKernelType BloomFilterCreateAbstractKernel::getType() const {
  return type_;
}

const std::vector<std::string> BloomFilterCreateAbstractKernel::getColumnNames() const {
  return columnNames_;
}

tl::expected<void, std::string>
BloomFilterCreateAbstractKernel::bufferTupleSet(const std::shared_ptr<TupleSet> &tupleSet) {
  // buffer tupleSet
  if (!receivedTupleSet_.has_value()) {
    receivedTupleSet_ = tupleSet;
  }
  else {
    // here we should use "concatenate" instead of "append" to avoid modification on the original tupleSet,
    // because the original one is also passed to downstream operators
    auto expConcatenatedTupleSet = TupleSet::concatenate({(*receivedTupleSet_), tupleSet});
    if (!expConcatenatedTupleSet.has_value()) {
      return tl::make_unexpected(expConcatenatedTupleSet.error());
    }
    receivedTupleSet_ = *expConcatenatedTupleSet;
  }
  return {};
}

void BloomFilterCreateAbstractKernel::clear() {
  receivedTupleSet_.reset();
}

}
