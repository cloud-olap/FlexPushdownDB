//
// Created by Yifei Yang on 12/13/21.
//

#include <fpdb/executor/physical/join/nestedloopjoin/RecordBatchNestedLoopJoiner.h>
#include <fpdb/expression/gandiva/Filter.h>
#include <fpdb/tuple/ArrayAppenderWrapper.h>
#include <fpdb/tuple/util/Util.h>

namespace fpdb::executor::physical::join {

RecordBatchNestedLoopJoiner::RecordBatchNestedLoopJoiner(const optional<shared_ptr<expression::gandiva::Expression>> &predicate,
                                                         const shared_ptr<::arrow::Schema> &outputSchema,
                                                         const vector<shared_ptr<pair<bool, int>>> &neededColumnIndice):
  predicate_(predicate),
  outputSchema_(outputSchema),
  joinedArrayVectors_{static_cast<uint>(outputSchema_->num_fields())} {

  // initialize required variables
  arrow::FieldVector leftFields, rightFields;
  for (uint c = 0; c < neededColumnIndice.size(); ++c) {
    const auto &pair = neededColumnIndice[c];
    const auto &field = outputSchema->field((int) c);
    if (pair->first) {
      neededLeftColumnIndexes_.emplace_back(pair->second);
      leftFields.emplace_back(field);
    } else {
      neededRightColumnIndexes_.emplace_back(pair->second);
      rightFields.emplace_back(field);
    }
  }
  leftOutputSchema_ = arrow::schema(leftFields);
  rightOutputSchema_ = arrow::schema(rightFields);
}

shared_ptr<RecordBatchNestedLoopJoiner>
RecordBatchNestedLoopJoiner::make(const optional<shared_ptr<Expression>> &predicate,
                                  const shared_ptr<::arrow::Schema> &outputSchema,
                                  const vector<shared_ptr<pair<bool, int>>> &neededColumnIndice) {
  return make_shared<RecordBatchNestedLoopJoiner>(predicate, 
                                                  outputSchema, 
                                                  neededColumnIndice);
}

tl::expected<void, string> RecordBatchNestedLoopJoiner::join(const shared_ptr<::arrow::RecordBatch> &leftRecordBatch,
                                                             const shared_ptr<::arrow::RecordBatch> &rightRecordBatch,
                                                             int64_t leftRowOffset,
                                                             int64_t rightRowOffset) {
  // compute cartesian product
  const auto &expCartesianBatch = cartesian(leftRecordBatch, rightRecordBatch);
  if (!expCartesianBatch.has_value()) {
    return tl::make_unexpected(expCartesianBatch.error());
  }
  const auto &cartesianBatch = expCartesianBatch.value();

  // skip for empty batches, filter empty batches using gandiva will throw errors
  if (cartesianBatch->num_rows() == 0) {
    return {};
  }

  // filter using predicate
  arrow::ArrayVector outputArrayVector;
  if (predicate_.has_value()) {
    const auto &expOutputArrayVector = filter(cartesianBatch,
                                              leftRecordBatch->num_rows(),
                                              leftRowOffset,
                                              rightRowOffset);
    if (!expOutputArrayVector.has_value()) {
      return tl::make_unexpected(expOutputArrayVector.error());
    }
    outputArrayVector = *expOutputArrayVector;
  } else {
    outputArrayVector = cartesianBatch->columns();
  }

  // buffer
  for (uint c = 0; c < outputArrayVector.size(); ++c) {
    joinedArrayVectors_[c].emplace_back(outputArrayVector[c]);
  }

  return {};
}

tl::expected<shared_ptr<arrow::RecordBatch>, string>
RecordBatchNestedLoopJoiner::cartesian(const shared_ptr<::arrow::RecordBatch> &leftRecordBatch,
                                       const shared_ptr<::arrow::RecordBatch> &rightRecordBatch) {
  // check empty first
  if (leftRecordBatch->num_rows() == 0 || rightRecordBatch->num_rows() == 0) {
    return fpdb::tuple::util::Util::makeEmptyRecordBatch(outputSchema_);
  }

  // in a high level, copy entire right batch [# left rows] times,
  // then, for left batch, copy each row [# right rows] times
  // e.g., left = [1,2,3,4], right = [a,b,c], after cartesian:
  // right = [a,b,c,a,b,c,a,b,c,a,b,c], left = [1,1,1,2,2,2,3,3,3,4,4,4]
  // left/right above may be reversed, based on which side has more rows, where we copy as a whole
  const auto &leftColumns = leftRecordBatch->columns();
  const auto &rightColumns = rightRecordBatch->columns();
  bool copyLeftWhole = leftRecordBatch->num_rows() > rightRecordBatch->num_rows();
  arrow::ArrayVector outArrays;

  // left
  for (int c: neededLeftColumnIndexes_) {
    auto expCopiedArray = copyLeftWhole ?
            fpdb::tuple::util::Util::copyArrayWhole(leftColumns[c], rightRecordBatch->num_rows()) :
            fpdb::tuple::util::Util::copyArrayRow(leftColumns[c], rightRecordBatch->num_rows());
    if (!expCopiedArray.has_value()) {
      return tl::make_unexpected(expCopiedArray.error());
    }
    outArrays.emplace_back(*expCopiedArray);
  }

  // right
  for (int c: neededRightColumnIndexes_) {
    auto expCopiedArray = copyLeftWhole ?
            fpdb::tuple::util::Util::copyArrayRow(rightColumns[c], leftRecordBatch->num_rows()) :
            fpdb::tuple::util::Util::copyArrayWhole(rightColumns[c], leftRecordBatch->num_rows());
    if (!expCopiedArray.has_value()) {
      return tl::make_unexpected(expCopiedArray.error());
    }
    outArrays.emplace_back(*expCopiedArray);
  }

  return arrow::RecordBatch::Make(outputSchema_, leftRecordBatch->num_rows() * rightRecordBatch->num_rows(), outArrays);
}

tl::expected<arrow::ArrayVector, string> RecordBatchNestedLoopJoiner::filter(const shared_ptr<arrow::RecordBatch> &recordBatch,
                                                       int64_t leftBatchNumRows,
                                                       int64_t leftRowOffset,
                                                       int64_t rightRowOffset) {
  // build filter if not yet
  if (!filter_.has_value()){
    filter_ = make_shared<fpdb::expression::gandiva::Filter>(predicate_.value());
    filter_.value()->compile(recordBatch->schema(), outputSchema_);
  }

  // filter the record batch, here we don't do it in one call, we first compute selection vector then project using it,
  // because we need the selection vector to compute left/right row match indexes for outer join
  const auto &expSelectionVector = filter_.value()->computeSelectionVector(*recordBatch);
  if (!expSelectionVector.has_value()) {
    return tl::make_unexpected(expSelectionVector.error());
  }
  const auto &selectionVector = *expSelectionVector;

  const auto &inputRowMatchIndexes = computeInputRowMatchIndexes(selectionVector,
                                                                 leftBatchNumRows,
                                                                 leftRowOffset,
                                                                 rightRowOffset);
  const auto &newLeftRowMatchIndexes = inputRowMatchIndexes.first;
  const auto &newRightRowMatchIndexes = inputRowMatchIndexes.second;
  leftRowMatchIndexes_.insert(newLeftRowMatchIndexes.begin(), newLeftRowMatchIndexes.end());
  rightRowMatchIndexes_.insert(newRightRowMatchIndexes.begin(), newRightRowMatchIndexes.end());

  // return filtered result
  return filter_.value()->evaluateBySelectionVector(*recordBatch, selectionVector);
}

pair<unordered_set<int64_t>, unordered_set<int64_t>>
RecordBatchNestedLoopJoiner::computeInputRowMatchIndexes(const shared_ptr<gandiva::SelectionVector> &selectionVector,
                                                         int64_t leftBatchNumRows,
                                                         int64_t leftRowOffset,
                                                         int64_t rightRowOffset) const {
  // compute the corresponding row index for both left and right inputs, for each matched output row
  unordered_set<int64_t> leftRowMatchIndexes, rightRowMatchIndexes;
  for (int64_t i = 0; i < selectionVector->GetNumSlots(); ++i) {
    uint64_t outputRow = selectionVector->GetIndex(i);
    leftRowMatchIndexes.emplace(outputRow % leftBatchNumRows + leftRowOffset);
    rightRowMatchIndexes.emplace(outputRow / leftBatchNumRows + rightRowOffset);
  }
  return make_pair(leftRowMatchIndexes, rightRowMatchIndexes);
}

tl::expected<shared_ptr<TupleSet>, string> RecordBatchNestedLoopJoiner::toTupleSet() {
  arrow::Status status;

  // make chunked arrays
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

const unordered_set<int64_t> &RecordBatchNestedLoopJoiner::getLeftRowMatchIndexes() const {
  return leftRowMatchIndexes_;
}

const unordered_set<int64_t> &RecordBatchNestedLoopJoiner::getRightRowMatchIndexes() const {
  return rightRowMatchIndexes_;
}

}
