//
// Created by Yifei Yang on 3/26/21.
//

#include <fpdb/executor/physical/shuffle/RecordBatchShuffler.h>
#include <fpdb/tuple/ArrayAppenderWrapper.h>
#include <fpdb/util/Util.h>
#include <utility>

using namespace fpdb::executor::physical::shuffle;
using namespace fpdb::tuple;
using namespace fpdb::util;

RecordBatchShuffler::RecordBatchShuffler(vector<int> shuffleColumnIndices,
                                           size_t numSlots,
                                           shared_ptr<::arrow::Schema> schema,
                                           vector<vector<shared_ptr<ArrayAppender>>> shuffledAppendersVector) :
  shuffleColumnIndices_(move(shuffleColumnIndices)),
  numSlots_(numSlots),
  schema_(move(schema)),
  shuffledAppendersVector_(move(shuffledAppendersVector)) {}

tl::expected<shared_ptr<RecordBatchShuffler>, string>
RecordBatchShuffler::make(const vector<string> &columnNames,
                           size_t numSlots,
                           const shared_ptr<::arrow::Schema> &schema,
                           size_t numRows) {

  // Get the shuffle column indexes, checking the column exists
  vector<int> shuffleColumnIndices;
  for (const auto &columnName: columnNames) {
    auto shuffleColumnIndex = schema->GetFieldIndex(ColumnName::canonicalize(columnName));
    if (shuffleColumnIndex == -1) {
      return tl::make_unexpected(fmt::format("Shuffle column '{}' does not exist", columnName));
    }
    shuffleColumnIndices.emplace_back(shuffleColumnIndex);
  }

  vector<vector<shared_ptr<ArrayAppender>>> shuffledAppendersVector{numSlots};
  // Create appenders
  for (size_t s = 0; s < numSlots; ++s) {
    shuffledAppendersVector[s] = vector<shared_ptr<ArrayAppender>>{static_cast<size_t>(schema->num_fields())};
    for (int c = 0; c < schema->num_fields(); ++c) {
      auto expectedAppender = ArrayAppenderBuilder::make(schema->field(c)->type(), numRows);
      if (!expectedAppender.has_value()) {
        return tl::make_unexpected(fmt::format("{}, RecordBatchShuffler", expectedAppender.error()));
      }
      shuffledAppendersVector[s][c] = expectedAppender.value();
    }
  }

  return make_shared<RecordBatchShuffler>(shuffleColumnIndices, numSlots, schema, shuffledAppendersVector);
}

tl::expected<void, string> RecordBatchShuffler::shuffle(const shared_ptr<::arrow::RecordBatch> &recordBatch) {

  // Get reference to the arrays to shuffle on, and get numRows, numColumns
  vector<shared_ptr<arrow::Array>> shuffleColumns;
  for (const auto &shuffleColumnIndex: shuffleColumnIndices_) {
    const auto &shuffleColumn = recordBatch->column(shuffleColumnIndex);
    shuffleColumns.emplace_back(shuffleColumn);
  }
  int64_t numRows = recordBatch->num_rows();
  int numColumns = recordBatch->num_columns();

  // Create hashers for the shuffle arrays
  vector<shared_ptr<ArrayHasher>> shuffleColumnHashers;
  for (const auto &shuffleColumn: shuffleColumns) {
    shared_ptr<ArrayHasher> shuffleColumnHasher;
    auto expectedShuffleColumnHasher = ArrayHasher::make(shuffleColumn);
    if (!expectedShuffleColumnHasher.has_value())
      return tl::make_unexpected(expectedShuffleColumnHasher.error());
    else
      shuffleColumnHasher = expectedShuffleColumnHasher.value();
    shuffleColumnHashers.emplace_back(shuffleColumnHasher);
  }


  // Generate partition indexes from the shuffle column
  vector<vector<int64_t>> partitionIndexes;
  for (size_t s = 0; s < numSlots_; ++s) {
    partitionIndexes.emplace_back(vector<int64_t>());
  }
  for (int64_t r = 0; r < numRows; ++r) {
    size_t partitionId = ArrayHasher::hash(shuffleColumnHashers, r) % numSlots_;
    partitionIndexes[partitionId].emplace_back(r);
  }

  // Shuffle the record batch into appenders
  for (int c = 0; c < numColumns; ++c) {
    auto column = recordBatch->column(c);
    for (size_t s = 0; s < numSlots_; ++s) {
      auto appender = shuffledAppendersVector_[s][c];
      for (auto r: partitionIndexes[s]) {
        appender->appendValue(column, r);
      }
    }
  }

  return {};
}

tl::expected<vector<shared_ptr<TupleSet>>, string> RecordBatchShuffler::toTupleSets() {

  // Finalize appenders into arrays
  vector<vector<shared_ptr<::arrow::Array>>> shuffledArrays{numSlots_};
  for (size_t s = 0; s < numSlots_; ++s) {
    shuffledArrays[s] = vector<shared_ptr<::arrow::Array>>{static_cast<size_t>(schema_->num_fields())};
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
  vector<shared_ptr<TupleSet>> shuffledTupleSetVector{numSlots_};
  for (size_t s = 0; s < numSlots_; ++s) {
    auto shuffledTable = ::arrow::Table::Make(schema_, shuffledArrays[s]);
    shuffledTupleSetVector[s] = TupleSet::make(shuffledTable);
  }

  return shuffledTupleSetVector;
}
