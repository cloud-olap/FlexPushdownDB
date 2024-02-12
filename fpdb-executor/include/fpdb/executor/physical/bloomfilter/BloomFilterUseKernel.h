//
// Created by Yifei Yang on 3/17/22.
//

#ifndef FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_BLOOMFILTER_BLOOMFILTERUSEKERNEL_H
#define FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_BLOOMFILTER_BLOOMFILTERUSEKERNEL_H

#include <fpdb/executor/physical/bloomfilter/BloomFilter.h>
#include <fpdb/executor/physical/bloomfilter/ArrowBloomFilter.h>
#include <fpdb/expression/gandiva/Filter.h>
#include <fpdb/tuple/TupleSet.h>
#include <fpdb/tuple/RecordBatchHasher.h>
#include <tl/expected.hpp>

using namespace fpdb::tuple;

namespace fpdb::executor::physical::bloomfilter {

class BloomFilterUseKernel {

public:
  static tl::expected<std::shared_ptr<TupleSet>, std::string>
  filter(const std::shared_ptr<TupleSet> &tupleSet,
         const std::shared_ptr<BloomFilterBase> &bloomFilter,
         const std::vector<std::string> &columnNames);

  static tl::expected<std::shared_ptr<TupleSet>, std::string>
  filter(const std::shared_ptr<TupleSet> &tupleSet,
         const std::shared_ptr<BloomFilter> &bloomFilter,
         const std::vector<std::string> &columnNames);

  static tl::expected<std::shared_ptr<TupleSet>, std::string>
  filter(const std::shared_ptr<TupleSet> &tupleSet,
         const std::shared_ptr<ArrowBloomFilter> &bloomFilter,
         const std::vector<std::string> &columnNames);

private:
  static tl::expected<::arrow::ArrayVector, std::string>
  filterRecordBatch(const ::arrow::RecordBatch &recordBatch,
                    const std::shared_ptr<BloomFilter> &bloomFilter,
                    const std::vector<int> &columnIndices);

  static tl::expected<std::shared_ptr<arrow::RecordBatch>, std::string>
  filterRecordBatch(const std::shared_ptr<arrow::RecordBatch> &recordBatch,
                    const std::shared_ptr<ArrowBloomFilter> &bloomFilter,
                    const std::shared_ptr<RecordBatchHasher> &hasher,
                    int64_t hardwareFlags);

};

}


#endif //FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_BLOOMFILTER_BLOOMFILTERUSEKERNEL_H
