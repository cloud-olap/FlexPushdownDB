//
// Created by Yifei Yang on 11/22/22.
//

#include <doctest/doctest.h>
#include <nanobench.h>
#include <memory>

#include <fpdb/executor/physical/bloomfilter/BloomFilterCreateKernel.h>
#include <fpdb/executor/physical/bloomfilter/BloomFilterCreateArrowKernel.h>
#include <fpdb/executor/physical/bloomfilter/BloomFilterUseKernel.h>
#include <fpdb/executor/physical/join/hashjoin/HashJoinArrowKernel.h>
#include <fpdb/tuple/util/Sample.h>
#include <fpdb/tuple/RecordBatchHasher.h>
#include <fpdb/tuple/arrow/exec/BloomFilter.h>
#include <arrow/compute/api_vector.h>

using namespace fpdb::executor::physical::bloomfilter;
using namespace fpdb::executor::physical::join;
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
  tl::expected<std::shared_ptr<TupleSet>, std::string> expFilteredTupleSet;
  switch (bloomFilter->getType()) {
    case BloomFilterType::VANILLA_BF: {
      // column indices
      auto expColumnIndices = BloomFilterUseKernel::makeColumnIndices(tupleSet->schema(), columnNames);
      if (!expColumnIndices.has_value()) {
        throw std::runtime_error(expColumnIndices.error());
      }
      // filter
      expFilteredTupleSet = BloomFilterUseKernel::filter(tupleSet,
                                                         std::static_pointer_cast<BloomFilter>(bloomFilter),
                                                         **expColumnIndices);
      break;
    }
    case BloomFilterType::ARROW_BF: {
      // hasher
      auto expHasher = RecordBatchHasher::make(tupleSet->schema(), columnNames);
      if (!expHasher.has_value()) {
        throw std::runtime_error(expHasher.error());
      }
      // filter
      expFilteredTupleSet = BloomFilterUseKernel::filter(
              tupleSet,
              std::static_pointer_cast<ArrowBloomFilter>(bloomFilter)->getBlockedBloomFilter(),
              *expHasher);
      break;
    }
    default: {
      throw std::runtime_error(fmt::format("Unsupported bloom filter type in micro-bench: {}", bloomFilter->getType()));
    }
  }
  if (!expFilteredTupleSet.has_value()) {
    throw std::runtime_error(expFilteredTupleSet.error());
  }
  return *expFilteredTupleSet;
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

void runArrowBloomFilterHashJoinCompare(
        const std::shared_ptr<TupleSet> &leftTupleSet, const std::shared_ptr<TupleSet> &rightTupleSet,
        const std::vector<std::string> &leftColumnNames, const std::vector<std::string> &rightColumnNames,
        const std::set<std::string> &neededColumnNames,
        std::vector<int64_t> &bfOutSizes, std::vector<int64_t> &hjOutSizes) {
  // bloom filter, basically copied from functions above
  bool bfOutRecorded = false;
  ankerl::nanobench::Config().minEpochIterations(1).run(
          fmt::format("[Arrow] bloomfilter-{}-{}-rows", leftTupleSet->numRows(), rightTupleSet->numRows()), [&] {
            // create
            auto kernel = BloomFilterCreateArrowKernel::make(leftColumnNames);
            auto res = kernel->bufferTupleSet(leftTupleSet);
            if (!res.has_value()) {
              throw std::runtime_error(res.error());
            }
            res = kernel->buildBloomFilter();
            if (!res.has_value()) {
              throw std::runtime_error(res.error());
            }
            auto bloomFilter = kernel->getBloomFilter();

            // use
            auto expHasher = RecordBatchHasher::make(rightTupleSet->schema(), rightColumnNames);
            if (!expHasher.has_value()) {
              throw std::runtime_error(expHasher.error());
            }
            auto expFilteredTupleSet = BloomFilterUseKernel::filter(
                    rightTupleSet,
                    std::static_pointer_cast<ArrowBloomFilter>(*bloomFilter)->getBlockedBloomFilter(),
                    *expHasher);
            if (!expFilteredTupleSet.has_value()) {
              throw std::runtime_error(expFilteredTupleSet.error());
            }
            if (!bfOutRecorded) {
              bfOutSizes.emplace_back((*expFilteredTupleSet)->numRows());
              bfOutRecorded = true;
            }
          });

  // hashjoin, basically copied from "HashJoinBenchmark"
  bool hjOutRecorded = false;
  ankerl::nanobench::Config().minEpochIterations(1).run(
          fmt::format("[Arrow] hashjoin-{}-{}-rows", leftTupleSet->numRows(), rightTupleSet->numRows()), [&] {
            auto kernel = HashJoinArrowKernel::make(HashJoinPredicate(leftColumnNames, rightColumnNames),
                                                    neededColumnNames, JoinType::INNER);

            // build
            auto res = kernel.joinBuildTupleSet(leftTupleSet);
            if (!res.has_value()) {
              throw std::runtime_error(res.error());
            }
            kernel.finalizeInput(true);

            // probe
            res = kernel.joinProbeTupleSet(rightTupleSet);
            if (!res.has_value()) {
              throw std::runtime_error(res.error());
            }
            kernel.finalizeInput(false);

            // output
            auto expOutputTupleSet = kernel.getOutputBuffer();
            if (!hjOutRecorded) {
              if (!expOutputTupleSet.has_value()) {
                hjOutSizes.emplace_back(0);
              }
              hjOutSizes.emplace_back((*expOutputTupleSet)->numRows());
              hjOutRecorded = true;
            }
          });
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

TEST_CASE ("bloomfilter-hashjoin-arrow-compare" * doctest::skip(false || SKIP_SUITE)) {
  std::vector<std::string> leftColumnNames{"c_0"};
  std::vector<std::string> rightColumnNames{"c_0"};
  std::set<std::string> neededColumnNames{"c_0", "c_1", "c_2"};
  auto dist10 = std::uniform_int_distribution(0, 10);
  auto dist100 = std::uniform_int_distribution(0, 100);
  auto dist1000 = std::uniform_int_distribution(0, 1000);
  auto dist10000 = std::uniform_int_distribution(0, 10000);
  auto dist100000 = std::uniform_int_distribution(0, 100000);
  auto dist1000000 = std::uniform_int_distribution(0, 1000000);
  auto dist10000000 = std::uniform_int_distribution(0, 10000000);
  auto tupleSet10 = Sample::sampleCxRInt<long, arrow::Int64Type>(10, 10, dist10);
  auto tupleSet100 = Sample::sampleCxRInt<long, arrow::Int64Type>(10, 100, dist100);
  auto tupleSet1000 = Sample::sampleCxRInt<long, arrow::Int64Type>(10, 1000, dist1000);
  auto tupleSet10000 = Sample::sampleCxRInt<long, arrow::Int64Type>(10, 10000, dist10000);
  auto tupleSet100000 = Sample::sampleCxRInt<long, arrow::Int64Type>(10, 100000, dist100000);
  auto tupleSet1000000 = Sample::sampleCxRInt<long, arrow::Int64Type>(10, 1000000, dist1000000);
  auto tupleSet10000000 = Sample::sampleCxRInt<long, arrow::Int64Type>(10, 10000000, dist10000000);

  std::vector<int64_t> bfOutSizes, hjOutSizes;
  runArrowBloomFilterHashJoinCompare(tupleSet10, tupleSet100, leftColumnNames, rightColumnNames,
                                     neededColumnNames, bfOutSizes, hjOutSizes);
  runArrowBloomFilterHashJoinCompare(tupleSet100, tupleSet1000, leftColumnNames, rightColumnNames,
                                     neededColumnNames, bfOutSizes, hjOutSizes);
  runArrowBloomFilterHashJoinCompare(tupleSet1000, tupleSet10000, leftColumnNames, rightColumnNames,
                                     neededColumnNames, bfOutSizes, hjOutSizes);
  runArrowBloomFilterHashJoinCompare(tupleSet10000, tupleSet100000, leftColumnNames, rightColumnNames,
                                     neededColumnNames, bfOutSizes, hjOutSizes);
  runArrowBloomFilterHashJoinCompare(tupleSet100000, tupleSet1000000, leftColumnNames, rightColumnNames,
                                     neededColumnNames, bfOutSizes, hjOutSizes);
  runArrowBloomFilterHashJoinCompare(tupleSet1000000, tupleSet10000000, leftColumnNames, rightColumnNames,
                                     neededColumnNames, bfOutSizes, hjOutSizes);

  // show output sizes
  printf("Output sizes:\n");
  for (uint i = 0; i < bfOutSizes.size(); ++i) {
    printf("%s\n", fmt::format("  BF: {}, HJ: {}", bfOutSizes[i], hjOutSizes[i]).c_str());
  }
}

}
