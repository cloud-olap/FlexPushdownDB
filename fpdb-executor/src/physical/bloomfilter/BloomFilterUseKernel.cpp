//
// Created by Yifei Yang on 3/17/22.
//

#include <fpdb/executor/physical/bloomfilter/BloomFilterUseKernel.h>
#include <fpdb/tuple/ArrayHasher.h>
#include <arrow/compute/api_vector.h>
#include <fmt/format.h>

namespace fpdb::executor::physical::bloomfilter {

tl::expected<std::shared_ptr<TupleSet>, std::string>
BloomFilterUseKernel::filter(const std::shared_ptr<TupleSet> &tupleSet,
                             const std::shared_ptr<BloomFilterBase> &bloomFilter,
                             const std::vector<std::string> &columnNames) {
  // Check
  if (tupleSet->numRows() == 0 || !bloomFilter->valid()) {
    return tupleSet;
  }

  switch (bloomFilter->getType()) {
    case BloomFilterType::BLOOM_FILTER: {
      return filter(tupleSet, std::static_pointer_cast<BloomFilter>(bloomFilter), columnNames);
    }
    case BloomFilterType::ARROW_BLOOM_FILTER: {
      return filter(tupleSet, std::static_pointer_cast<ArrowBloomFilter>(bloomFilter), columnNames);
    }
    default: {
      return tl::make_unexpected(fmt::format("Unknown bloom filter type: {}", bloomFilter->getType()));
    }
  }
}

tl::expected<std::shared_ptr<TupleSet>, std::string>
BloomFilterUseKernel::filter(const std::shared_ptr<TupleSet> &tupleSet,
                             const std::shared_ptr<BloomFilter> &bloomFilter,
                             const std::vector<std::string> &columnNames) {
  // Make column indices
  std::vector<int> columnIndices;
  auto schema = tupleSet->schema();
  for (const auto &columnName: columnNames) {
    auto columnIndex = schema->GetFieldIndex(ColumnName::canonicalize(columnName));
    if (columnIndex == -1) {
      return tl::make_unexpected(fmt::format("Column '{}' does not exist", columnName));
    }
    columnIndices.emplace_back(columnIndex);
  }

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
                             const std::shared_ptr<ArrowBloomFilter> &bloomFilter,
                             const std::vector<std::string> &columnNames) {
  // hasher
  auto expHasher = RecordBatchHasher::make(tupleSet->schema(), columnNames);
  if (!expHasher.has_value()) {
    return tl::make_unexpected(expHasher.error());
  }
  auto hasher = *expHasher;

  // filter batches
  int64_t hardwareFlags = arrow::internal::CpuInfo::GetInstance()->hardware_flags();
  arrow::RecordBatchVector filteredBatches;
  arrow::TableBatchReader reader{*tupleSet->table()};
  auto expRecordBatch = reader.Next();
  if (!expRecordBatch.ok()) {
    return tl::make_unexpected(expRecordBatch.status().message());
  }
  auto recordBatch = *expRecordBatch;
  while (recordBatch) {
    auto expFilteredBatch = filterRecordBatch(recordBatch, bloomFilter, hasher, hardwareFlags);
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
                                        const std::shared_ptr<ArrowBloomFilter> &bloomFilter,
                                        const std::shared_ptr<RecordBatchHasher> &hasher,
                                        int64_t hardwareFlags) {
  // hash
  int64_t batchSize = recordBatch->num_rows();
  uint32_t* hashes = (uint32_t*) malloc(sizeof(uint32_t) * batchSize);
  hasher->hash(recordBatch, hashes);

  // filter
  uint8_t* bitVector = (uint8_t*) malloc(batchSize);
  bloomFilter->getBlockedBloomFilter()->Find(hardwareFlags, batchSize, hashes, bitVector);
  auto selectBuffer = std::make_unique<arrow::Buffer>(bitVector, arrow::BitUtil::BytesForBits(batchSize));
  arrow::ArrayData selectArrayData(arrow::boolean(), batchSize, {nullptr, std::move(selectBuffer)});
  auto expDatum = arrow::compute::Filter(arrow::Datum(recordBatch), arrow::Datum(selectArrayData));
  if (!expDatum.ok()) {
    // clear
    free(hashes);
    free(bitVector);
    return tl::make_unexpected(expDatum.status().message());
  }

  // clear
  free(hashes);
  free(bitVector);
  return (*expDatum).record_batch();
}

}
