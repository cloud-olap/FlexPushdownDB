//
// Created by Yifei Yang on 2/5/24.
//

#include <fpdb/executor/physical/split/SplitKernel.h>

using namespace fpdb::tuple;

namespace fpdb::executor::physical::split {

// used by "split()"
arrow::ArrayVector splitArray(const std::shared_ptr<arrow::Array> &array, uint n) {
  int64_t splitSize = array->length() / n;
  arrow::ArrayVector splitArrays{n};
  for (uint i = 0; i < n; ++i) {
    int64_t offset = i * splitSize;
    if (i < n - 1) {
      splitArrays[i] = array->Slice(offset, splitSize);
    } else {
      splitArrays[i] = array->Slice(offset);
    }
  }
  return splitArrays;
}

tl::expected<std::vector<std::shared_ptr<tuple::TupleSet>>, std::string>
SplitKernel::split(const std::shared_ptr<tuple::TupleSet> &tupleSet, uint n) {
  // special case if n == 1
  if (n == 1) {
    return std::vector<std::shared_ptr<TupleSet>>{tupleSet};
  }

  // check empty
  std::vector<std::shared_ptr<TupleSet>> output{n};
  const auto &schema = tupleSet->schema();
  if (tupleSet->numRows() == 0){
    for (uint i = 0; i < n; ++i) {
      output[i] = TupleSet::make(schema);
    }
    return output;
  }

  // combine chunks
  const auto &inputTable = tupleSet->table();
  const auto &expCombinedTable = inputTable->CombineChunks();
  if (!expCombinedTable.ok()) {
    return tl::make_unexpected(expCombinedTable.status().message());
  }
  const auto &combinedTable = *expCombinedTable;

  // split
  std::vector<arrow::ArrayVector> outputArrayVectors{n};
  for (const auto &column: combinedTable->columns()) {
    const auto &inputArray = column->chunk(0);
    const auto &splitArrays = splitArray(inputArray, n);
    for (uint i = 0; i < n; ++i) {
      outputArrayVectors[i].emplace_back(splitArrays[i]);
    }
  }

  // make tables
  for (uint i = 0; i < n; ++i) {
    output[i] = TupleSet::make(schema, outputArrayVectors[i]);
  }
  return output;
}

tl::expected<std::vector<std::shared_ptr<tuple::TupleSet>>, std::string>
SplitKernel::split2(const std::shared_ptr<tuple::TupleSet> &tupleSet, uint n) {
  // special case if n == 1
  if (n == 1) {
    return std::vector<std::shared_ptr<TupleSet>>{tupleSet};
  }

  // check empty
  std::vector<std::shared_ptr<TupleSet>> output{n};
  const auto &schema = tupleSet->schema();
  if (tupleSet->numRows() == 0){
    for (uint i = 0; i < n; ++i) {
      output[i] = TupleSet::make(schema);
    }
    return output;
  }

  // compute num rows per split, and how many splits have 1 more row
  int64_t numRows = tupleSet->numRows();
  int64_t numRowsPerSplit = numRows / n;
  uint remainder = numRows % n;

  // read input into batches
  arrow::TableBatchReader reader(*tupleSet->table());
  auto expBatches = reader.ToRecordBatches();
  if (!expBatches.ok()) {
    return tl::make_unexpected(expBatches.status().message());
  }
  auto batches = *expBatches;

  // split
  int batchId = 0, batchOff = 0;
  for (uint i = 0; i < n; ++i) {
    arrow::RecordBatchVector outBatchesThisSplit;
    int numRowsThisSplit = numRowsPerSplit + (i < remainder);
    // handle empty split first
    if (numRowsThisSplit == 0) {
      output[i] = TupleSet::make(tupleSet->schema());
      continue;
    }
    // keep saving batches when the current batch is not large enough to finalize the current split
    while (numRowsThisSplit > 0 && numRowsThisSplit >= batches[batchId]->num_rows() - batchOff) {
      outBatchesThisSplit.emplace_back((batchOff == 0) ? batches[batchId] : batches[batchId]->Slice(batchOff));
      numRowsThisSplit -= (batches[batchId]->num_rows() - batchOff);
      ++batchId;
      batchOff = 0;
    }
    // finalize the remainder of the current split using portion of the current batch
    if (numRowsThisSplit > 0) {
      outBatchesThisSplit.emplace_back(batches[batchId]->Slice(batchOff, numRowsThisSplit));
      batchOff += numRowsThisSplit;
    }
    // form the output table
    auto expOutTable = arrow::Table::FromRecordBatches(outBatchesThisSplit);
    if (!expOutTable.ok()) {
      return tl::make_unexpected(expOutTable.status().message());
    }
    output[i] = TupleSet::make(*expOutTable);
  }

  return output;
}

tl::expected<std::vector<std::shared_ptr<tuple::TupleSet>>, std::string>
SplitKernel::splitForOverFlow(const std::shared_ptr<tuple::TupleSet> &tupleSet) {
  int64_t sizeBytes = tupleSet->size();
  int intMax = std::numeric_limits<int32_t>::max();
  if (sizeBytes < intMax) {
    return std::vector<std::shared_ptr<tuple::TupleSet>>{tupleSet};
  }
  // ceil(sizeBytes / intMax) + 1
  uint n = (sizeBytes + intMax - 1) / intMax + 1;
  return split2(tupleSet, n);
}

}
