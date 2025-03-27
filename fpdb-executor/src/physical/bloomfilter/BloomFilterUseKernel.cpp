//
// Created by Yifei Yang on 3/17/22.
//

#include <fpdb/executor/physical/bloomfilter/BloomFilterUseKernel.h>
#include <fpdb/executor/physical/bloomfilter/ArrowBloomFilter.h>
#include <fpdb/executor/physical/bloomfilter/GlobalArrowBloomFilter.h>
#include <fpdb/tuple/ArrayHasher.h>
#include <arrow/compute/api_vector.h>
#include <fmt/format.h>

namespace fpdb::executor::physical::bloomfilter {

tl::expected<std::shared_ptr<TupleSet>, std::string>
BloomFilterUseKernel::filter(const std::shared_ptr<TupleSet> &tupleSet,
                             const std::shared_ptr<BloomFilter> &bloomFilter,
                             const std::vector<int> &columnIndices) {
  std::vector<::arrow::ArrayVector> filteredArrayVectors;
  for (int c = 0; c < tupleSet->numColumns(); ++c) {
    filteredArrayVectors.emplace_back(::arrow::ArrayVector{});
  }

  ::arrow::Result<std::shared_ptr<::arrow::RecordBatch>> recordBatchResult;
  ::arrow::Status status;
  ::arrow::TableBatchReader reader(*tupleSet->table());
  reader.set_chunksize(DefaultChunkSize);

  // Read a batch
  recordBatchResult = reader.Next();
  if (!recordBatchResult.ok()) {
    return tl::make_unexpected(recordBatchResult.status().message());
  }
  auto recordBatch = *recordBatchResult;

  while (recordBatch) {
    // Filter
    auto expArrayVector = filterRecordBatch(*recordBatch, bloomFilter, columnIndices);
    if (!expArrayVector.has_value()) {
      return tl::make_unexpected(expArrayVector.error());
    }
    auto arrayVector = *expArrayVector;
    for (int c = 0; c < tupleSet->numColumns(); ++c) {
      filteredArrayVectors[c].emplace_back(arrayVector[c]);
    }

    // Read a batch
    recordBatchResult = reader.Next();
    if (!recordBatchResult.ok()) {
      return tl::make_unexpected(recordBatchResult.status().message());
    }
    recordBatch = *recordBatchResult;
  }

  // Make result tupleSet
  ::arrow::ChunkedArrayVector chunkedArrayVector;
  for (int c = 0; c < tupleSet->numColumns(); ++c) {
    auto expChunkedArray = ::arrow::ChunkedArray::Make(filteredArrayVectors[c]);
    if (!expChunkedArray.ok()) {
      return tl::make_unexpected(expChunkedArray.status().message());
    }
    chunkedArrayVector.emplace_back(*expChunkedArray);
  }

  return TupleSet::make(tupleSet->schema(), chunkedArrayVector);
}

tl::expected<std::shared_ptr<TupleSet>, std::string>
BloomFilterUseKernel::filter(const std::shared_ptr<TupleSet> &tupleSet,
                             const std::shared_ptr<arrow::compute::BlockedBloomFilter> &bloomFilter,
                             const std::shared_ptr<RecordBatchHasher> &hasher) {
  // filter batches
  arrow::RecordBatchVector filteredBatches;
  arrow::TableBatchReader reader{*tupleSet->table()};
  auto expRecordBatch = reader.Next();
  if (!expRecordBatch.ok()) {
    return tl::make_unexpected(expRecordBatch.status().message());
  }
  auto recordBatch = *expRecordBatch;
  while (recordBatch) {
    auto expFilteredBatch = filterRecordBatch(recordBatch, bloomFilter, hasher);
    if (!expFilteredBatch.has_value()) {
      return tl::make_unexpected(expFilteredBatch.error());
    }
    filteredBatches.emplace_back(*expFilteredBatch);

    expRecordBatch = reader.Next();
    if (!expRecordBatch.ok()) {
      return tl::make_unexpected(expRecordBatch.status().message());
    }
    recordBatch = *expRecordBatch;
  }

  // make output tupleSet
  auto expTable = arrow::Table::FromRecordBatches(filteredBatches);
  if (!expTable.ok()) {
    return tl::make_unexpected(expTable.status().message());
  }
  return TupleSet::make(*expTable);
}

tl::expected<std::shared_ptr<std::vector<int>>, std::string>
BloomFilterUseKernel::makeColumnIndices(const std::shared_ptr<arrow::Schema> &schema,
                                        const std::vector<std::string> &columnNames) {
  auto columnIndices = std::make_shared<std::vector<int>>();
  for (const auto &columnName: columnNames) {
    auto columnIndex = schema->GetFieldIndex(ColumnName::canonicalize(columnName));
    if (columnIndex == -1) {
      return tl::make_unexpected(fmt::format("Column '{}' does not exist", columnName));
    }
    columnIndices->emplace_back(columnIndex);
  }
  return columnIndices;
}

tl::expected<::arrow::ArrayVector, std::string>
BloomFilterUseKernel::filterRecordBatch(const ::arrow::RecordBatch &recordBatch,
                                        const std::shared_ptr<BloomFilter> &bloomFilter,
                                        const std::vector<int> &columnIndices) {
  // Create hashers to get int value of different types
  std::vector<std::shared_ptr<ArrayHasher>> columnHashers;
  for (const auto &columnIndex: columnIndices) {
    auto expColumnHasher = ArrayHasher::make(recordBatch.column(columnIndex));
    if (!expColumnHasher.has_value()) {
      return tl::make_unexpected(expColumnHasher.error());
    }
    columnHashers.emplace_back(*expColumnHasher);
  }

  // Filter
  std::shared_ptr<::gandiva::SelectionVector> selectionVector;
  auto status = ::gandiva::SelectionVector::MakeInt64(recordBatch.num_rows(),
                                                      ::arrow::default_memory_pool(),
                                                      &selectionVector);
  if (!status.ok()) {
    return tl::make_unexpected(status.message());
  }

  int64_t slotId = 0;
  for (int64_t r = 0; r < recordBatch.num_rows(); ++r) {
    int64_t hash = ArrayHasher::hash(columnHashers, r);

    if (bloomFilter->contains(hash)) {
      selectionVector->SetIndex(slotId++, r);
    }
  }
  selectionVector->SetNumSlots(slotId);

  if (slotId == recordBatch.num_rows()) {
    return recordBatch.columns();
  } else {
    return fpdb::expression::gandiva::Filter::evaluateBySelectionVectorStatic(recordBatch, selectionVector);
  }
}

tl::expected<std::shared_ptr<arrow::RecordBatch>, std::string>
BloomFilterUseKernel::filterRecordBatch(const std::shared_ptr<arrow::RecordBatch> &recordBatch,
                                        const std::shared_ptr<arrow::compute::BlockedBloomFilter> &bloomFilter,
                                        const std::shared_ptr<RecordBatchHasher> &hasher) {
  int64_t batchSize = recordBatch->num_rows();
  int64_t bitvecLen = arrow::bit_util::BytesForBits(batchSize);
  uint8_t* bitVector = (uint8_t*) malloc(bitvecLen);

  // use 32/64-bit hashes according to the bloom filter
  if (bloomFilter->use_64bit_hashes()) {
    // hash
    uint64_t* hashes = (uint64_t*) malloc(sizeof(uint64_t) * batchSize);
    hasher->hash(recordBatch, hashes);
    // filter to get bitvec
    bloomFilter->Find(hasher->getHardwareFlags(), batchSize, hashes, bitVector);
    free(hashes);
  } else {
    // hash
    uint32_t* hashes = (uint32_t*) malloc(sizeof(uint32_t) * batchSize);
    hasher->hash(recordBatch, hashes);
    // filter to get bitvec
    bloomFilter->Find(hasher->getHardwareFlags(), batchSize, hashes, bitVector);
    free(hashes);
  }

  // apply the bitvector to get filtered output
  auto selectBuffer = std::make_unique<arrow::Buffer>(bitVector, bitvecLen);
  arrow::ArrayData selectArrayData(arrow::boolean(), batchSize, {nullptr, std::move(selectBuffer)});
  auto expDatum = arrow::compute::Filter(arrow::Datum(recordBatch), arrow::Datum(selectArrayData));
  free(bitVector);
  if (!expDatum.ok()) {
    return tl::make_unexpected(expDatum.status().message());
  }
  return (*expDatum).record_batch();
}

}
