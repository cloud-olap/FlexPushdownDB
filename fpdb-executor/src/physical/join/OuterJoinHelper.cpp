//
// Created by Yifei Yang on 12/17/21.
//

#include <fpdb/executor/physical/join/OuterJoinHelper.h>
#include <fpdb/expression/gandiva/Filter.h>
#include <fpdb/tuple/ColumnBuilder.h>

namespace fpdb::executor::physical::join {

OuterJoinHelper::OuterJoinHelper(bool isLeft,
                                 const shared_ptr<arrow::Schema> &outputSchema,
                                 const vector<shared_ptr<pair<bool, int>>> &neededColumnIndice):
  isLeft_(isLeft),
  outputSchema_(outputSchema),
  neededColumnIndice_(neededColumnIndice) {}

shared_ptr<OuterJoinHelper> OuterJoinHelper::make(bool isLeft,
                                                  const shared_ptr<arrow::Schema> &outputSchema,
                                                  const vector<shared_ptr<pair<bool, int>>> &neededColumnIndice) {
  return make_shared<OuterJoinHelper>(isLeft, outputSchema, neededColumnIndice);
}

void OuterJoinHelper::putRowMatchIndexes(const unordered_set<int64_t> &newRowMatchIndexes) {
  if (!rowMatchIndexes_.has_value()) {
    rowMatchIndexes_ = newRowMatchIndexes;
  } else {
    rowMatchIndexes_->insert(newRowMatchIndexes.begin(), newRowMatchIndexes.end());
  }
}

tl::expected<shared_ptr<TupleSet>, string> OuterJoinHelper::compute(const shared_ptr<TupleSet> &tupleSet) {
  // keep side
  const auto &expKeepArrayVector = computeKeepSide(tupleSet);
  if (!expKeepArrayVector) {
    return tl::make_unexpected(expKeepArrayVector.error());
  }
  const auto &keepArrayVector = expKeepArrayVector.value();

  // discard side
  const auto &expDiscardArrayVector = computeDiscardSide(tupleSet);
  if (!expDiscardArrayVector) {
    return tl::make_unexpected(expDiscardArrayVector.error());
  }
  const auto &discardArrayVector = expDiscardArrayVector.value();

  // make output
  arrow::ArrayVector outputArrayVector;
  if (isLeft_) {
    outputArrayVector.insert(outputArrayVector.end(), keepArrayVector.begin(), keepArrayVector.end());
    outputArrayVector.insert(outputArrayVector.end(), discardArrayVector.begin(), discardArrayVector.end());
  } else {
    outputArrayVector.insert(outputArrayVector.end(), discardArrayVector.begin(), discardArrayVector.end());
    outputArrayVector.insert(outputArrayVector.end(), keepArrayVector.begin(), keepArrayVector.end());
  }
  return TupleSet::make(outputSchema_, outputArrayVector);
}

tl::expected<arrow::ArrayVector, string> OuterJoinHelper::computeKeepSide(const shared_ptr<TupleSet> &tupleSet) {
  // collect needed column indexes of the keep side
  vector<int> columnIds;
  for (const auto &pair: neededColumnIndice_) {
    if ((isLeft_ && pair->first) || (!isLeft_ && !pair->first)) {
        columnIds.emplace_back(pair->second);
    }
  }
  // check if no columns needed
  if (columnIds.empty()) {
    return {};
  }

  // project needed columns
  const auto &expProjectTupleSet = tupleSet->project(columnIds);
  if (!expProjectTupleSet.has_value()) {
    return tl::make_unexpected(expProjectTupleSet.error());
  }
  const auto &projectTupleSet = expProjectTupleSet.value();

  // combine and make the record batch
  auto result = projectTupleSet->combine();
  if (!result.has_value()) {
    return tl::make_unexpected(result.error());
  }
  arrow::ArrayVector inputArrayVector;
  for (const auto &column: projectTupleSet->table()->columns()) {
    inputArrayVector.emplace_back(column->chunk(0));
  }

  // if no rowMatchIndexes, then all rows are unmatched, then simply return inputArrayVector
  if (!rowMatchIndexes_.has_value() || rowMatchIndexes_.value().empty()) {
    return inputArrayVector;
  }

  // make input recordBatch
  const auto &recordBatch = arrow::RecordBatch::Make(projectTupleSet->schema(),
                                                     projectTupleSet->numRows(),
                                                     inputArrayVector);

  // make selection vector
  int64_t vectorLength = tupleSet->numRows() - (int64_t) rowMatchIndexes_->size();
  shared_ptr<::gandiva::SelectionVector> selectionVector;
  auto status = ::gandiva::SelectionVector::MakeInt64(vectorLength, ::arrow::default_memory_pool(), &selectionVector);
  if (!status.ok()) {
    return tl::make_unexpected(status.message());
  }
  selectionVector->SetNumSlots(vectorLength);
  int64_t slotId = 0;
  for (int64_t r = 0; r < recordBatch->num_rows(); ++r) {
    if (rowMatchIndexes_->find(r) == rowMatchIndexes_->end()) {
      selectionVector->SetIndex(slotId++, r);
    }
  }

  // filter to get unmatched rows
  return fpdb::expression::gandiva::Filter::evaluateBySelectionVectorStatic(*recordBatch, selectionVector);
}

tl::expected<arrow::ArrayVector, string> OuterJoinHelper::computeDiscardSide(const shared_ptr<TupleSet> &keepTupleSet) {
  // make column builders for discard side
  vector<shared_ptr<ColumnBuilder>> columnBuilders;
  int discardNumColumns = 0;
  for (const auto &pair: neededColumnIndice_) {
    if ((isLeft_ && !pair->first) || (!isLeft_ && pair->first)) {
      ++discardNumColumns;
    }
  }
  // check if no columns needed
  if (discardNumColumns == 0) {
    return {};
  }
  pair<int, int> discardColumnIdRange = isLeft_ ?
          make_pair(outputSchema_->num_fields() - discardNumColumns, outputSchema_->num_fields()) :
          make_pair(0, discardNumColumns);
  for (int c = discardColumnIdRange.first; c < discardColumnIdRange.second; ++c) {
    const auto &field = outputSchema_->field(c);
    columnBuilders.emplace_back(ColumnBuilder::make(field->name(), field->type()));
  }

  // append nulls
  int64_t length = rowMatchIndexes_.has_value() ?
          keepTupleSet->numRows() - (int64_t) rowMatchIndexes_->size() : keepTupleSet->numRows();
  for (const auto &builder: columnBuilders) {
    auto result = builder->appendNulls(length);
    if (!result.has_value()) {
      return tl::make_unexpected(result.error());
    }
  }

  // finalize to arrays
  arrow::ArrayVector outputArrayVector;
  for (const auto &builder: columnBuilders) {
    const auto &expArray = builder->finalizeToArray();
    if (!expArray.has_value()) {
      return tl::make_unexpected(expArray.error());
    }
    outputArrayVector.emplace_back(expArray.value());
  }

  return outputArrayVector;
}

}
