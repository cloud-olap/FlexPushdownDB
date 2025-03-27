//
// Created by Yifei Yang on 3/17/22.
//

#ifndef FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_BLOOMFILTER_BLOOMFILTERUSEKERNEL_H
#define FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_BLOOMFILTER_BLOOMFILTERUSEKERNEL_H

#include <fpdb/executor/physical/bloomfilter/BloomFilter.h>
#include <fpdb/expression/gandiva/Filter.h>
#include <fpdb/tuple/TupleSet.h>
#include <fpdb/tuple/RecordBatchHasher.h>
#include <fpdb/tuple/arrow/exec/BloomFilter.h>
#include <tl/expected.hpp>

using namespace fpdb::tuple;

namespace fpdb::executor::physical::bloomfilter {

/**
 * Caller needs to check if table is empty and bloom filter is valid
 */
class BloomFilterUseKernel {

public:
  // use vanilla bloom filter (reuse column indices made by the caller)
  static tl::expected<std::shared_ptr<TupleSet>, std::string>
  filter(const std::shared_ptr<TupleSet> &tupleSet,
         const std::shared_ptr<BloomFilter> &bloomFilter,
         const std::vector<int> &columnIndices);

  // use arrow blocked bloom filter (reuse hasher made by the caller)
  static tl::expected<std::shared_ptr<TupleSet>, std::string>
  filter(const std::shared_ptr<TupleSet> &tupleSet,
         const std::shared_ptr<arrow::compute::BlockedBloomFilter> &bloomFilter,
         const std::shared_ptr<RecordBatchHasher> &hasher);

  // prepare column indices (used by vanilla bloom filter)
  static tl::expected<std::shared_ptr<std::vector<int>>, std::string>
  makeColumnIndices(const std::shared_ptr<arrow::Schema> &schema,
                    const std::vector<std::string> &columnNames);

private:
  // using vanilla bloom filter (reuse column indices made by the caller)
  static tl::expected<::arrow::ArrayVector, std::string>
  filterRecordBatch(const ::arrow::RecordBatch &recordBatch,
                    const std::shared_ptr<BloomFilter> &bloomFilter,
                    const std::vector<int> &columnIndices);

  // using arrow blocked bloom filter (reuse hasher made by the caller)
  static tl::expected<std::shared_ptr<arrow::RecordBatch>, std::string>
  filterRecordBatch(const std::shared_ptr<arrow::RecordBatch> &recordBatch,
                    const std::shared_ptr<arrow::compute::BlockedBloomFilter> &bloomFilter,
                    const std::shared_ptr<RecordBatchHasher> &hasher);
};

}


#endif //FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_BLOOMFILTER_BLOOMFILTERUSEKERNEL_H
