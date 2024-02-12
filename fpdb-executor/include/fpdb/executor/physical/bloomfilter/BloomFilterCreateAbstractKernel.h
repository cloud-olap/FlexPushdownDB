//
// Created by Yifei Yang on 11/23/22.
//

#ifndef FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_BLOOMFILTER_BLOOMFILTERCREATEABSTRACTKERNEL_H
#define FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_BLOOMFILTER_BLOOMFILTERCREATEABSTRACTKERNEL_H

#include <fpdb/executor/physical/bloomfilter/BloomFilterBase.h>
#include <fpdb/tuple/TupleSet.h>
#include <tl/expected.hpp>
#include <vector>
#include <string>

using namespace fpdb::tuple;

namespace fpdb::executor::physical::bloomfilter {

enum BloomFilterCreateKernelType {
  BLOOM_FILTER_KERNEL,
  BLOOM_FILTER_ARROW_KERNEL
};

class BloomFilterCreateAbstractKernel {

public:
  BloomFilterCreateAbstractKernel(BloomFilterCreateKernelType type, const std::vector<std::string> &columnNames);
  BloomFilterCreateAbstractKernel() = default;
  BloomFilterCreateAbstractKernel(const BloomFilterCreateAbstractKernel&) = default;
  BloomFilterCreateAbstractKernel& operator=(const BloomFilterCreateAbstractKernel&) = default;
  virtual ~BloomFilterCreateAbstractKernel() = default;

  BloomFilterCreateKernelType getType() const;
  const std::vector<std::string> getColumnNames() const;

  tl::expected<void, std::string> bufferTupleSet(const std::shared_ptr<TupleSet> &tupleSet);
  virtual tl::expected<void, std::string> buildBloomFilter() = 0;
  virtual std::optional<std::shared_ptr<BloomFilterBase>> getBloomFilter() const = 0;

  virtual void clear();

protected:
  BloomFilterCreateKernelType type_;
  std::vector<std::string> columnNames_;

  std::optional<std::shared_ptr<TupleSet>> receivedTupleSet_;
};

}


#endif //FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_BLOOMFILTER_BLOOMFILTERCREATEABSTRACTKERNEL_H
