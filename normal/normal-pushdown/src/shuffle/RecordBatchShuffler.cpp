//
// Created by matt on 29/7/20.
//

#include "normal/pushdown/shuffle/RecordBatchShuffler.h"
#include "normal/pushdown/shuffle/ArrayHasher.h"
#include "normal/pushdown/shuffle/ArrayAppender.h"

#include <fmt/format.h>

#include <utility>

using namespace normal::pushdown::shuffle;

RecordBatchShuffler::RecordBatchShuffler(int shuffleColumnIndex,
										 size_t numSlots,
										 std::shared_ptr<::arrow::Schema> schema) :
	shuffleColumnIndex_(shuffleColumnIndex),
	numSlots_(numSlots),
	schema_(std::move(schema)),
	shuffledArraysVector_{numSlots} {

  // Initialise the destination vectors of arrays
  for (size_t s = 0; s < numSlots; ++s) {
	shuffledArraysVector_[s] =
		std::vector<std::vector<std::shared_ptr<::arrow::Array>>>{static_cast<size_t>(schema_->num_fields())};
  }
}

tl::expected<std::shared_ptr<RecordBatchShuffler>, std::string>
RecordBatchShuffler::make(const std::string &columnName,
						  size_t numSlots,
						  const std::shared_ptr<::arrow::Schema> &schema) {

  // Get the shuffle column index, checking the column exists
  auto shuffleColumnIndex = schema->GetFieldIndex(columnName);
  if (shuffleColumnIndex == -1) {
	return tl::make_unexpected(fmt::format("Shuffle column '{}' does not exist", columnName));
  }

  return std::make_shared<RecordBatchShuffler>(shuffleColumnIndex, numSlots, schema);
}

tl::expected<void, std::string> RecordBatchShuffler::shuffle(const std::shared_ptr<::arrow::RecordBatch> &recordBatch) {

  arrow::Status status;

  // Get an reference to the array to shuffle on
  const auto &shuffleColumn = std::static_pointer_cast<::arrow::StringArray>(recordBatch->column(shuffleColumnIndex_));

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

  // Create appenders to create the destination arrays
  std::vector<std::vector<std::shared_ptr<ArrayAppender>>> appenders{numSlots_};
  for (size_t s = 0; s < numSlots_; ++s) {
	appenders[s] = std::vector<std::shared_ptr<ArrayAppender>>{static_cast<size_t>(recordBatch->num_columns())};
	for (int c = 0; c < recordBatch->num_columns(); ++c) {
	  int64_t expectedSize = std::ceil(((double)recordBatch->num_rows() / (double)numSlots_) * 1.25);
	  auto expectedAppender = ArrayAppender::make(recordBatch->schema()->field(c)->type(), expectedSize);
	  if (!expectedAppender.has_value())
		return tl::make_unexpected(expectedAppender.error());
	  else
		appenders[s][c] = expectedAppender.value();
	}
  }

  // Shuffle the record batch into the buffers
  for (int64_t i = 0; i < shuffleColumn->length(); ++i) {
	auto partitionIndex = shuffleColumnHasher->hash(i) % numSlots_;
	for (int c = 0; c < recordBatch->num_columns(); ++c) {
	  appenders[partitionIndex][c]->appendValue(columns[c], i);
	}
  }

  // Create arrays from the appenders
  for (size_t s = 0; s < numSlots_; ++s) {
	for (size_t c = 0; c < appenders[s].size(); ++c) {
	  auto expectedArray = appenders[s][c]->finalize();
	  if (!expectedArray.has_value())
		return tl::make_unexpected(status.message());
	  else
		shuffledArraysVector_[s][c].emplace_back(expectedArray.value());
	}
  }

  return {};
}

tl::expected<std::vector<std::shared_ptr<TupleSet2>>, std::string> RecordBatchShuffler::toTupleSets() {

  arrow::Status status;

  // Create TupleSets from the destination vectors of arrays
  std::vector<std::shared_ptr<TupleSet2>> shuffledTupleSetVector{numSlots_};
  for (size_t s = 0; s < shuffledArraysVector_.size(); ++s) {

	std::vector<std::shared_ptr<::arrow::ChunkedArray>> chunkedArrays;
	for (const auto &columnArrays : shuffledArraysVector_[s]) {
	  auto chunkedArray = std::make_shared<::arrow::ChunkedArray>(columnArrays);
	  chunkedArrays.emplace_back(chunkedArray);
	}

	std::shared_ptr<::arrow::Table> shuffledTable = ::arrow::Table::Make(schema_, chunkedArrays);
	shuffledTupleSetVector[s] = TupleSet2::make(shuffledTable);
  }

  return shuffledTupleSetVector;
}
