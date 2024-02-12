//
// Created by Yifei Yang on 11/22/22.
//

#include <doctest/doctest.h>
#include <nanobench.h>
#include <memory>

#include <fpdb/executor/physical/bloomfilter/BloomFilterCreateKernel.h>
#include <fpdb/executor/physical/bloomfilter/BloomFilterCreateArrowKernel.h>
#include <fpdb/executor/physical/bloomfilter/BloomFilterUseKernel.h>
#include <fpdb/tuple/util/Sample.h>
#include <fpdb/tuple/RecordBatchHasher.h>
#include <fpdb/tuple/arrow/exec/BloomFilter.h>
#include <arrow/compute/api_vector.h>

using namespace fpdb::executor::physical::bloomfilter;
using namespace fpdb::tuple::util;
using namespace fpdb::tuple;

#define SKIP_SUITE false

std::shared_ptr<BloomFilterBase> create(const std::shared_ptr<TupleSet> &tupleSet,
                                        const std::vector<std::string> &columnNames,
                                        bool useNew) {
  std::shared_ptr<BloomFilterCreateAbstractKernel> kernel;
  if (useNew) {
    kernel = BloomFilterCreateArrowKernel::make(columnNames);
  } else {
    kernel = BloomFilterCreateKernel::make(columnNames, BloomFilter::DefaultDesiredFalsePositiveRate);
  }
  auto res = kernel->bufferTupleSet(tupleSet);
  if (!res.has_value()) {
    throw std::runtime_error(res.error());
  }
  res = kernel->buildBloomFilter();
  if (!res.has_value()) {
    throw std::runtime_error(res.error());
  }
  auto bloomFilter = kernel->getBloomFilter();
  if (!bloomFilter.has_value()) {
    throw std::runtime_error("Bloom filter not built!");
  }
  return *bloomFilter;
}

std::shared_ptr<TupleSet> use(const std::shared_ptr<TupleSet> &tupleSet,
                              const std::vector<std::string> &columnNames,
                              const std::shared_ptr<BloomFilterBase> &bloomFilter) {
  auto expTupleSet = BloomFilterUseKernel::filter(tupleSet, bloomFilter, columnNames);
  if (!expTupleSet.has_value()) {
    throw std::runtime_error(expTupleSet.error());
  }
  return *expTupleSet;
}

void runCreate(const std::shared_ptr<TupleSet> &tupleSet, const std::vector<std::string> &columnNames) {
  ankerl::nanobench::Config().minEpochIterations(1).run(
          fmt::format("[Original] bloomfilter-create-{}-rows", tupleSet->numRows()), [&] {
            auto bloomFilter = create(tupleSet, columnNames, false);
          });

  ankerl::nanobench::Config().minEpochIterations(1).run(
          fmt::format("[New] bloomfilter-create-{}-rows", tupleSet->numRows()), [&] {
            auto bloomFilter = create(tupleSet, columnNames, true);
          });
}

void runUse(const std::shared_ptr<TupleSet> &tupleSet, const std::vector<std::string> &columnNames,
            const std::shared_ptr<BloomFilterBase> &originalBloomFilter,
            const std::shared_ptr<BloomFilterBase> &newBloomFilter) {
  std::shared_ptr<TupleSet> outputTupleSet;

  ankerl::nanobench::Config().minEpochIterations(1).run(
          fmt::format("[Original] bloomfilter-use-{}-rows", tupleSet->numRows()), [&] {
            outputTupleSet = use(tupleSet, columnNames, originalBloomFilter);
          });

  SPDLOG_DEBUG("Output:\n{}", outputTupleSet->showString(TupleSetShowOptions(TupleSetShowOrientation::RowOriented)));

  ankerl::nanobench::Config().minEpochIterations(1).run(
          fmt::format("[New] bloomfilter-use-{}-rows", tupleSet->numRows()), [&] {
            outputTupleSet = use(tupleSet, columnNames, newBloomFilter);
          });

  SPDLOG_DEBUG("Output:\n{}", outputTupleSet->showString(TupleSetShowOptions(TupleSetShowOrientation::RowOriented)));
}

TEST_SUITE ("bloomfilter-benchmark" * doctest::skip(SKIP_SUITE)) {

TEST_CASE ("bloomfilter-create-benchmark" * doctest::skip(false || SKIP_SUITE)) {
  std::vector<std::string> columnNames{"c_0"};
  auto tupleSet10 = Sample::sampleCxRInt<long, arrow::Int64Type>(10, 10, std::uniform_int_distribution(0, 20));
  auto tupleSet100 = Sample::sampleCxRInt<long, arrow::Int64Type>(10, 100, std::uniform_int_distribution(0, 200));
  auto tupleSet1000 = Sample::sampleCxRInt<long, arrow::Int64Type>(10, 1000, std::uniform_int_distribution(0, 2000));
  auto tupleSet10000 = Sample::sampleCxRInt<long, arrow::Int64Type>(10, 10000, std::uniform_int_distribution(0, 20000));
  auto tupleSet100000 = Sample::sampleCxRInt<long, arrow::Int64Type>(10, 100000, std::uniform_int_distribution(0, 200000));
  auto tupleSet1000000 = Sample::sampleCxRInt<long, arrow::Int64Type>(10, 1000000, std::uniform_int_distribution(0, 2000000));

  runCreate(tupleSet10, columnNames);
  runCreate(tupleSet100, columnNames);
  runCreate(tupleSet1000, columnNames);
  runCreate(tupleSet10000, columnNames);
  runCreate(tupleSet100000, columnNames);
  runCreate(tupleSet1000000, columnNames);
}

TEST_CASE ("bloomfilter-use-benchmark" * doctest::skip(false || SKIP_SUITE)) {
  std::vector<std::string> columnNames{"c_0"};
  auto useTupleSet10 = Sample::sampleCxRInt<long, arrow::Int64Type>(10, 10, std::uniform_int_distribution(0, 2000));
  auto useTupleSet100 = Sample::sampleCxRInt<long, arrow::Int64Type>(10, 100, std::uniform_int_distribution(0, 2000));
  auto useTupleSet1000 = Sample::sampleCxRInt<long, arrow::Int64Type>(10, 1000, std::uniform_int_distribution(0, 2000));
  auto useTupleSet10000 = Sample::sampleCxRInt<long, arrow::Int64Type>(10, 10000, std::uniform_int_distribution(0, 2000));
  auto useTupleSet100000 = Sample::sampleCxRInt<long, arrow::Int64Type>(10, 100000, std::uniform_int_distribution(0, 2000));
  auto useTupleSet1000000 = Sample::sampleCxRInt<long, arrow::Int64Type>(10, 1000000, std::uniform_int_distribution(0, 2000));

  auto createTupleSet = Sample::sampleCxRInt<long, arrow::Int64Type>(10, 10000, std::uniform_int_distribution(0, 1000));
  auto originalBloomFilter = create(createTupleSet, columnNames, false);
  auto newBloomFilter = create(createTupleSet, columnNames, true);

  runUse(useTupleSet10, columnNames, originalBloomFilter, newBloomFilter);
  runUse(useTupleSet100, columnNames, originalBloomFilter, newBloomFilter);
  runUse(useTupleSet1000, columnNames, originalBloomFilter, newBloomFilter);
  runUse(useTupleSet10000, columnNames, originalBloomFilter, newBloomFilter);
  runUse(useTupleSet100000, columnNames, originalBloomFilter, newBloomFilter);
  runUse(useTupleSet1000000, columnNames, originalBloomFilter, newBloomFilter);
}

}
