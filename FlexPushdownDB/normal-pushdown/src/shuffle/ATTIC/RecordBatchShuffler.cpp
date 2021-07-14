//
// Created by matt on 29/7/20.
//

#include "normal/pushdown/shuffle/ATTIC/RecordBatchShuffler.h"
#include "normal/tuple/ArrayHasher.h"
#include "normal/tuple/ArrayAppender.h"

#include <fmt/format.h>

#include <utility>
#include <normal/tuple/ArrayAppenderWrapper.h>

using namespace normal::pushdown::shuffle;
using namespace normal::tuple;

RecordBatchShuffler::RecordBatchShuffler(int shuffleColumnIndex,
										 size_t numSlots,
										 std::shared_ptr<::arrow::Schema> schema,
										 size_t numRows) :
	shuffleColumnIndex_(shuffleColumnIndex),
	numSlots_(numSlots),
	schema_(std::move(schema)),
	shuffledAppendersVector_{numSlots},
  shuffledArraysVector_{numSlots} {

  // Create appenders
  for (size_t s = 0; s < numSlots; ++s) {
    shuffledAppendersVector_[s] = std::vector<std::shared_ptr<ArrayAppender>>{static_cast<size_t>(schema_->num_fields())};
    for (int c = 0; c < schema_->num_fields(); ++c) {
      auto expectedAppender = ArrayAppenderBuilder::make(schema_->field(c)->type(), DefaultChunkSize);
      if (!expectedAppender.has_value()) {
        throw std::runtime_error(fmt::format("{}, RecordBatchShuffler", expectedAppender.error()));
      }
      shuffledAppendersVector_[s][c] = expectedAppender.value();
    }
  }

  // Initialise the destination vectors of arrays
  for (size_t s = 0; s < numSlots; ++s) {
    shuffledArraysVector_[s] =
            std::vector<std::vector<std::shared_ptr<::arrow::Array>>>{static_cast<size_t>(schema_->num_fields())};
  }

  // Initialise value number counter
  for (size_t s = 0; s < numSlots_; ++s) {
    bufferedValueNums.emplace_back(std::vector<size_t>());
    for (int c = 0; c < schema_->num_fields(); ++c) {
      bufferedValueNums[s].emplace_back(0);
    }
  }
}

tl::expected<std::shared_ptr<RecordBatchShuffler>, std::string>
RecordBatchShuffler::make(const std::string &columnName,
						  size_t numSlots,
						  const std::shared_ptr<::arrow::Schema> &schema,
						  size_t numRows) {

  // Get the shuffle column index, checking the column exists
  auto shuffleColumnIndex = schema->GetFieldIndex(ColumnName::canonicalize(columnName));
  if (shuffleColumnIndex == -1) {
	return tl::make_unexpected(fmt::format("Shuffle column '{}' does not exist", columnName));
  }

  return std::make_shared<RecordBatchShuffler>(shuffleColumnIndex, numSlots, schema, numRows);
}

tl::expected<void, std::string> RecordBatchShuffler::shuffle(const std::shared_ptr<::arrow::RecordBatch> &recordBatch) {

  // Get an reference to the array to shuffle on
  const auto &shuffleColumn = recordBatch->column(shuffleColumnIndex_);

  // Create a hasher for the shuffle array
  std::shared_ptr<ArrayHasher> shuffleColumnHasher;
  auto expectedShuffleColumnHasher = ArrayHasher::make(shuffleColumn);
  if (!expectedShuffleColumnHasher.has_value())
	return tl::make_unexpected(expectedShuffleColumnHasher.error());
  else
	shuffleColumnHasher = expectedShuffleColumnHasher.value();

  // Create references each array in the record batch
  std::vector<std::shared_ptr<::arrow::Array>> columns{static_cast<size_t>(recordBatch->num_columns())};
  for (int c = 0; c < recordBatch->num_columns(); ++c) {
	columns[c] = recordBatch->column(c);
  }

  // Shuffle the record batch into appenders
  for (int64_t i = 0; i < shuffleColumn->length(); ++i) {
    auto partitionIndex = shuffleColumnHasher->hash(i) % numSlots_;
    for (int c = 0; c < recordBatch->num_columns(); ++c) {
      auto appender = shuffledAppendersVector_[partitionIndex][c];
      appender->appendValue(columns[c], i);

      // if has more than DefaultChunkSize values, create an array
      if (++bufferedValueNums[partitionIndex][c] >= DefaultChunkSize) {
        auto expectedArray = appender->finalize();
        if (!expectedArray.has_value()) {
          return tl::make_unexpected(expectedArray.error());
        }
        shuffledArraysVector_[partitionIndex][c].emplace_back(expectedArray.value());
        auto expectedAppender = ArrayAppenderBuilder::make(schema_->field(c)->type(), DefaultChunkSize);
        if (!expectedAppender.has_value()) {
          throw std::runtime_error(fmt::format("{}, RecordBatchShuffler", expectedAppender.error()));
        }
        shuffledAppendersVector_[partitionIndex][c] = expectedAppender.value();
        bufferedValueNums[partitionIndex][c] = 0;
      }
    }
  }

  return {};
}

tl::expected<std::vector<std::shared_ptr<TupleSet2>>, std::string> RecordBatchShuffler::toTupleSets() {

  // Finalize the rest appenders
  for (size_t s = 0; s < numSlots_; ++s) {
    for (int c = 0; c < schema_->num_fields(); ++c) {
      auto expectedArray = shuffledAppendersVector_[s][c]->finalize();
      if (!expectedArray.has_value()) {
        return tl::make_unexpected(expectedArray.error());
      }
      if (expectedArray.value()->length() > 0) {
        shuffledArraysVector_[s][c].emplace_back(expectedArray.value());
      }
    }
  }

  // Create TupleSets from the destination vectors of arrays
  std::vector<std::shared_ptr<TupleSet2>> shuffledTupleSetVector{numSlots_};
  for (size_t s = 0; s < shuffledArraysVector_.size(); ++s) {

    // check empty
    if (shuffledArraysVector_[s][0].empty()) {
      shuffledTupleSetVector[s] = TupleSet2::make(Schema::make(schema_));
    }

    else {
      std::vector<std::shared_ptr<::arrow::ChunkedArray>> chunkedArrays;
      for (const auto &columnArrays : shuffledArraysVector_[s]) {
        auto chunkedArray = std::make_shared<::arrow::ChunkedArray>(columnArrays);
        chunkedArrays.emplace_back(chunkedArray);
      }
      std::shared_ptr<::arrow::Table> shuffledTable = ::arrow::Table::Make(schema_, chunkedArrays);
      shuffledTupleSetVector[s] = TupleSet2::make(shuffledTable);
    }
  }

  return shuffledTupleSetVector;
}
