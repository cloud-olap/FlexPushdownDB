//
// Created by matt on 3/8/20.
//

#include <fpdb/executor/physical/join/hashjoin/RecordBatchHashJoiner.h>
#include <fpdb/tuple/TupleSetIndexFinder.h>
#include <fpdb/tuple/ArrayAppenderWrapper.h>
#include <fpdb/tuple/TupleKey.h>

using namespace fpdb::executor::physical::join;

RecordBatchHashJoiner::RecordBatchHashJoiner(shared_ptr<TupleSetIndex> buildTupleSetIndex,
                                             vector<string> probeJoinColumnNames,
                                             shared_ptr<::arrow::Schema> outputSchema,
                                             vector<shared_ptr<pair<bool, int>>> neededColumnIndice,
                                             int64_t buildRowOffset) :
	buildTupleSetIndex_(move(buildTupleSetIndex)),
	probeJoinColumnNames_(move(probeJoinColumnNames)),
	outputSchema_(move(outputSchema)),
	neededColumnIndice_(move(neededColumnIndice)),
  joinedArrayVectors_{static_cast<size_t>(outputSchema_->num_fields())},
	buildRowOffset_(buildRowOffset) {}

tl::expected<shared_ptr<RecordBatchHashJoiner>, string>
RecordBatchHashJoiner::make(const shared_ptr<TupleSetIndex> &buildTupleSetIndex,
                            const vector<string> &probeJoinColumnNames,
                            const shared_ptr<::arrow::Schema> &outputSchema,
                            const vector<shared_ptr<pair<bool, int>>> &neededColumnIndice,
                            int64_t buildRowOffset) {
  const auto &canonicalColumnNames = ColumnName::canonicalize(probeJoinColumnNames);
  return make_shared<RecordBatchHashJoiner>(buildTupleSetIndex,
                                            canonicalColumnNames,
                                            outputSchema,
                                            neededColumnIndice,
                                            buildRowOffset);
}

tl::expected<void, string> RecordBatchHashJoiner::join(const shared_ptr<::arrow::RecordBatch> &probeRecordBatch,
                                                       int64_t probeRowOffset) {

  arrow::Status status;

  // Combine the chunks in the build table so we have single arrays for each column
  auto combineResult = buildTupleSetIndex_->combine();
  if (!combineResult)
    return tl::make_unexpected(combineResult.error());

  //  buildTupleSetIndex_->validate();

  auto buildTable = buildTupleSetIndex_->getTupleSet()->table();

  // Create a tupleSetIndexFinder
  const auto &expectedIndexFinder = TupleSetIndexFinder::make(buildTupleSetIndex_,
                                                              probeJoinColumnNames_,
                                                              probeRecordBatch);
  if (!expectedIndexFinder.has_value())
	  return tl::make_unexpected(expectedIndexFinder.error());
  auto indexFinder = expectedIndexFinder.value();

  // Create references to each array in the index
  ::arrow::ArrayVector buildColumns;
  for (const auto &column: buildTable->columns()) {
    buildColumns.emplace_back(column->chunk(0));
  }

  // Create references to each array in the record batch
  vector<shared_ptr<::arrow::Array>> probeColumns{static_cast<size_t>(probeRecordBatch->num_columns())};
  for (int c = 0; c < probeRecordBatch->num_columns(); ++c) {
    probeColumns[c] = probeRecordBatch->column(c);
  }

  // create appenders to create the destination arrays
  vector<shared_ptr<ArrayAppender>> appenders{static_cast<size_t>(outputSchema_->num_fields())};

  for (int c = 0; c < outputSchema_->num_fields(); ++c) {
    auto expectedAppender = ArrayAppenderBuilder::make(outputSchema_->field(c)->type(), 0);
    if (!expectedAppender.has_value())
      return tl::make_unexpected(expectedAppender.error());
    appenders[c] = expectedAppender.value();
  }

  // Iterate through the probe join column
  for (int64_t pr = 0; pr < probeRecordBatch->num_rows(); ++pr) {

    // Find matched rows in the build column
    const auto &expBuildRows = indexFinder->find(pr);
    if (!expBuildRows.has_value()) {
      return tl::make_unexpected(expBuildRows.error());
    }
    const auto &buildRows = expBuildRows.value();

    // Save probeRowMatchIndexes
    if (!buildRows.empty()) {
      probeRowMatchIndexes_.emplace(probeRowOffset + pr);
    }

    // Iterate the matched rows in the build column
    for (const auto br: buildRows) {

      // Save buildRowMatchIndexes
      buildRowMatchIndexes_.emplace(buildRowOffset_ + br);

      // Iterate needed columns
      for (size_t c = 0; c < neededColumnIndice_.size(); ++c) {

        // build column
        if (neededColumnIndice_[c]->first) {
          auto appendResult = appenders[c]->appendValue(buildColumns[neededColumnIndice_[c]->second], br);
          if(!appendResult) return appendResult;
        }

        // probe column
        else {
          auto appendResult = appenders[c]->appendValue(probeColumns[neededColumnIndice_[c]->second], pr);
          if(!appendResult) return appendResult;
        }
      }
    }
  }

  // Create arrays from the appenders
  for (size_t c = 0; c < appenders.size(); ++c) {
    auto expectedArray = appenders[c]->finalize();
    if (!expectedArray.has_value())
      return tl::make_unexpected(expectedArray.error());
    if (expectedArray.value()->length() > 0)
      joinedArrayVectors_[c].emplace_back(expectedArray.value());
  }

  return {};
}

tl::expected<shared_ptr<TupleSet>, string>
RecordBatchHashJoiner::toTupleSet() {
  arrow::Status status;

  // Make chunked arrays
  vector<shared_ptr<::arrow::ChunkedArray>> chunkedArrays;
  for (const auto &joinedArrayVector: joinedArrayVectors_) {
    // check empty
    if (joinedArrayVector.empty()) {
      return TupleSet::make(outputSchema_);
    }

    auto chunkedArray = make_shared<::arrow::ChunkedArray>(joinedArrayVector);
    chunkedArrays.emplace_back(chunkedArray);
  }

  auto joinedTable = ::arrow::Table::Make(outputSchema_, chunkedArrays);
  auto joinedTupleSet = TupleSet::make(joinedTable);

  joinedArrayVectors_.clear();
  return joinedTupleSet;
}

const unordered_set<int64_t> &RecordBatchHashJoiner::getBuildRowMatchIndexes() const {
  return buildRowMatchIndexes_;
}

const unordered_set<int64_t> &RecordBatchHashJoiner::getProbeRowMatchIndexes() const {
  return probeRowMatchIndexes_;
}
