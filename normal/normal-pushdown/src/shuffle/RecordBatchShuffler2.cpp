//
// Created by Yifei Yang on 3/26/21.
//

#include "normal/pushdown/shuffle/RecordBatchShuffler2.h"
#include "normal/tuple/ArrayHasher.h"
#include "normal/tuple/ArrayAppender.h"


#include <utility>
#include <normal/tuple/ArrayAppenderWrapper.h>

using namespace normal::pushdown::shuffle;
using namespace normal::tuple;

RecordBatchShuffler2::RecordBatchShuffler2(int shuffleColumnIndex,
                                         size_t numSlots,
                                         std::shared_ptr<::arrow::Schema> schema,
                                         size_t numRows) :
  shuffleColumnIndex_(shuffleColumnIndex),
  numSlots_(numSlots),
  schema_(std::move(schema)),
  shuffledAppendersVector_{numSlots} {

  // Create appenders
  for (size_t s = 0; s < numSlots_; ++s) {
    shuffledAppendersVector_[s] = std::vector<std::shared_ptr<ArrayAppender>>{static_cast<size_t>(schema_->num_fields())};
    for (int c = 0; c < schema_->num_fields(); ++c) {
      auto expectedAppender = ArrayAppenderBuilder::make(schema_->field(c)->type(), numRows);
      if (!expectedAppender.has_value()) {
        throw std::runtime_error(fmt::format("{}, RecordBatchShuffler", expectedAppender.error()));
      }
      shuffledAppendersVector_[s][c] = expectedAppender.value();
    }
  }
}

tl::expected<std::shared_ptr<RecordBatchShuffler2>, std::string>
RecordBatchShuffler2::make(const std::string &columnName,
                          size_t numSlots,
                          const std::shared_ptr<::arrow::Schema> &schema,
                          size_t numRows) {

  // Get the shuffle column index, checking the column exists
  auto shuffleColumnIndex = schema->GetFieldIndex(ColumnName::canonicalize(columnName));
  if (shuffleColumnIndex == -1) {
    return tl::make_unexpected(fmt::format("Shuffle column '{}' does not exist", columnName));
  }

  return std::make_shared<RecordBatchShuffler2>(shuffleColumnIndex, numSlots, schema, numRows);
}

tl::expected<void, std::string> RecordBatchShuffler2::shuffle(const std::shared_ptr<::arrow::RecordBatch> &recordBatch) {

  // Get an reference to the array to shuffle on, and get numRows, numColumns
  const auto &shuffleColumn = recordBatch->column(shuffleColumnIndex_);
  size_t numRows = recordBatch->num_rows(), numColumns = recordBatch->num_columns();

  // Create a hasher for the shuffle array
  std::shared_ptr<ArrayHasher> shuffleColumnHasher;
  auto expectedShuffleColumnHasher = ArrayHasher::make(shuffleColumn);
  if (!expectedShuffleColumnHasher.has_value())
    return tl::make_unexpected(expectedShuffleColumnHasher.error());
  else
    shuffleColumnHasher = expectedShuffleColumnHasher.value();


  // Generate partition indexes from the shuffle column
  std::vector<std::vector<size_t>> partitionIndexes1;
  for (size_t s = 0; s < numSlots_; ++s) {
    partitionIndexes1.emplace_back(std::vector<size_t>());
  }
  for (size_t r = 0; r < numRows; ++r) {
    size_t partitionId = shuffleColumnHasher->hash(r) % numSlots_;
    partitionIndexes1[partitionId].emplace_back(r);
  }

  // Shuffle the record batch into appenders
  for (size_t c = 0; c < numColumns; ++c) {
    auto column = recordBatch->column(c);
    for (size_t s = 0; s < numSlots_; ++s) {
      auto appender = shuffledAppendersVector_[s][c];
      for (auto r: partitionIndexes1[s]) {
        appender->appendValue(column, r);
      }
    }
  }

  return {};
}

tl::expected<std::vector<std::shared_ptr<TupleSet2>>, std::string> RecordBatchShuffler2::toTupleSets() {

  // Finalize appenders into arrays
  std::vector<std::vector<std::shared_ptr<::arrow::Array>>> shuffledArrays{numSlots_};
  for (size_t s = 0; s < numSlots_; ++s) {
    shuffledArrays[s] = std::vector<std::shared_ptr<::arrow::Array>>{static_cast<size_t>(schema_->num_fields())};
  }
  for (size_t s = 0; s < numSlots_; ++s) {
    for (int c = 0; c < schema_->num_fields(); ++c) {
      auto expectedArray = shuffledAppendersVector_[s][c]->finalize();
      if (!expectedArray.has_value()) {
        return tl::make_unexpected(expectedArray.error());
      }
      shuffledArrays[s][c] = expectedArray.value();
    }
  }

  // Create TupleSets from the destination vectors of arrays
  std::vector<std::shared_ptr<TupleSet2>> shuffledTupleSetVector{numSlots_};
  for (size_t s = 0; s < numSlots_; ++s) {
    auto shuffledTable = ::arrow::Table::Make(schema_, shuffledArrays[s]);
    shuffledTupleSetVector[s] = TupleSet2::make(shuffledTable);
  }

  return shuffledTupleSetVector;
}