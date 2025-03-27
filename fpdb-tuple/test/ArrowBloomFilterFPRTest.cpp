//
// Created by Yifei Yang on 2/8/24.
//

#include <fpdb/tuple/arrow/exec/BloomFilter.h>
#include <fpdb/tuple/util/Sample.h>
#include <fpdb/tuple/RecordBatchHasher.h>
#include <doctest/doctest.h>
#include <unordered_set>

using namespace fpdb::tuple;
using namespace fpdb::tuple::util;
using namespace arrow::compute;

#define SKIP_SUITE false

void makeTestData(int nInsert, int nFind, uint64_t* inserts, uint64_t* finds) {
  std::random_device rd;
  std::mt19937 gen(rd());
  auto dist = std::uniform_int_distribution(0, INT_MAX);
  std::unordered_set<uint64_t> values;
  while ((int) values.size() < nInsert + nFind) {
    values.emplace(dist(gen));
  }
  int i = 0;
  for (uint64_t v: values) {
    if (i < nInsert) {
      inserts[i++] = v;
    } else {
      finds[i++ - nInsert] = v;
    }
  }
}

void hash(int n, uint64_t* values, uint64_t* hashes) {
  // make record batch
  auto schema = std::make_shared<arrow::Schema>(
          arrow::FieldVector{std::make_shared<arrow::Field>("A", arrow::uint64())});
  auto buffer = std::make_unique<arrow::Buffer>((uint8_t*) values, n * sizeof(uint64_t));
  auto arrayData = std::make_shared<arrow::ArrayData>(
          arrow::uint64(), n, arrow::BufferVector {nullptr, std::move(buffer)});
  auto recordBatch = arrow::RecordBatch::Make(schema, n, arrow::ArrayDataVector{arrayData});

  // make hasher
  auto expHasher = RecordBatchHasher::make(schema, {"A"});
  if (!expHasher.has_value()) {
    throw std::runtime_error(expHasher.error());
  }

  // hash
  (*expHasher)->hash(recordBatch, hashes);
}

void run(int nInsert, int nFind) {
  printf("[%d inserts, %d finds]:\n", nInsert, nFind);
  const int nIter = 5;
  std::vector<int> fps;

  // run multiple times
  for (int it = 0; it < nIter; ++it) {
    // make test data
    uint64_t *inserts = new uint64_t[nInsert];
    uint64_t *finds = new uint64_t[nFind];
    makeTestData(nInsert, nFind, inserts, finds);

    // inserts
    auto bloomFilter = std::make_shared<BlockedBloomFilter>();
    auto bfBuilder = BloomFilterBuilder::Make(BloomFilterBuildStrategy::SINGLE_THREADED);
    auto hardwareFlag = arrow::internal::CpuInfo::GetInstance()->hardware_flags();
    auto status = bfBuilder->Begin(1, hardwareFlag, arrow::default_memory_pool(), nInsert, 1, 0, bloomFilter.get());
    if (!status.ok()) {
      throw std::runtime_error(status.message());
    }
    uint64_t *hashes = new uint64_t[nInsert];
    hash(nInsert, inserts, hashes);
    status = bfBuilder->PushNextBatch(0, nInsert, hashes);
    if (!status.ok()) {
      throw std::runtime_error(status.message());
    }
    delete[] inserts;
    delete[] hashes;

    // finds
    uint8_t *bitVector = (uint8_t *) malloc(arrow::bit_util::BytesForBits(nFind));
    hashes = new uint64_t[nFind];
    hash(nFind, finds, hashes);
    bloomFilter->Find(hardwareFlag, nFind, hashes, bitVector);
    delete[] finds;

    // compute FPR
    int fp = 0;
    for (int i = 0; i < nFind; ++i) {
      fp += arrow::bit_util::GetBit(bitVector, i);
    }
    fps.emplace_back(fp);
  }

  // show results
  double max_fpr = (double (*std::max_element(fps.begin(), fps.end()))) / nFind;
  double min_fpr = (double (*std::min_element(fps.begin(), fps.end()))) / nFind;
  double avg_fpr = ((double) std::accumulate(fps.begin(), fps.end(), 0)) / nFind / nIter;
  const auto midIt = fps.begin() + nIter / 2;
  double med_fpr;
  std::nth_element(fps.begin(), midIt, fps.end());
  if (nIter % 2 == 0) {
    const auto leftMidIt = std::max_element(fps.begin(), midIt);
    med_fpr = (((double) *leftMidIt + (double) *midIt) / 2) / nFind;
  } else {
    med_fpr = ((double) *midIt) / nFind;
  }
  printf("avg = %.2f%%, med = %.2f%%, max = %.2f%%, min = %.2f%%\n\n",
         avg_fpr * 100, med_fpr * 100, max_fpr * 100, min_fpr * 100);
}

TEST_SUITE ("bloomfilter-fpr" * doctest::skip(SKIP_SUITE)) {

TEST_CASE ("bloomfilter-fpr-same-num-insert-find" * doctest::skip(false || SKIP_SUITE)) {
  std::vector<int> nInserts{10000, 20000, 30000, 40000, 50000, 60000, 70000, 80000, 90000,
                            100000, 200000, 300000, 400000, 500000, 600000, 700000, 800000, 900000, 1000000};
  for (int nInsert: nInserts) {
    run(nInsert, nInsert);
  }
}

TEST_CASE ("bloomfilter-fpr-fix-num-find" * doctest::skip(false || SKIP_SUITE)) {
  std::vector<int> nInserts{10000, 20000, 30000, 40000, 50000, 60000, 70000, 80000, 90000,
                            100000, 200000, 300000, 400000, 500000, 600000, 700000, 800000, 900000, 1000000};
  int nFind = 100000;
  for (int nInsert: nInserts) {
    run(nInsert, nFind);
  }
}

TEST_CASE ("bloomfilter-fpr-fix-num-insert" * doctest::skip(false || SKIP_SUITE)) {
  std::vector<int> nFinds{10000, 20000, 30000, 40000, 50000, 60000, 70000, 80000, 90000,
                          100000, 200000, 300000, 400000, 500000, 600000, 700000, 800000, 900000, 1000000};
  int nInsert = 100000;
  for (int nFind: nFinds) {
    run(nInsert, nFind);
  }
}

}