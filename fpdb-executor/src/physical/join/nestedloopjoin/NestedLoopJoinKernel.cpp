//
// Created by Yifei Yang on 12/12/21.
//

#include <fpdb/executor/physical/join/nestedloopjoin/NestedLoopJoinKernel.h>
#include <fpdb/executor/physical/join/nestedloopjoin/RecordBatchNestedLoopJoiner.h>

namespace fpdb::executor::physical::join {

NestedLoopJoinKernel::NestedLoopJoinKernel(const std::optional<shared_ptr<expression::gandiva::Expression>> &predicate,
                                           const set<string> &neededColumnNames,
                                           bool isLeft,
                                           bool isRight):
  predicate_(predicate),
  neededColumnNames_(neededColumnNames),
  isLeft_(isLeft),
  isRight_(isRight) {}

NestedLoopJoinKernel NestedLoopJoinKernel::make(const std::optional<shared_ptr<expression::gandiva::Expression>> &predicate,
                                                const set<string> &neededColumnNames,
                                                bool isLeft,
                                                bool isRight) {
  return {predicate, neededColumnNames, isLeft, isRight};
}

tl::expected<void, string> bufferInput(std::optional<shared_ptr<TupleSet>> &buffer,
                                       const shared_ptr<TupleSet> &incomingTupleSet) {
  if (!buffer.has_value()) {
    buffer = incomingTupleSet;
    return {};
  } else if (incomingTupleSet->numRows() > 0) {
    return buffer.value()->append(incomingTupleSet);
  } else {
    return {};
  }
}

tl::expected<shared_ptr<TupleSet>, string> NestedLoopJoinKernel::join(const shared_ptr<TupleSet> &leftTupleSet,
                                                                      const shared_ptr<TupleSet> &rightTupleSet) {
  // create joiner
  int64_t leftRowOffset = leftTupleSet_.has_value() ? leftTupleSet_.value()->numRows() : 0;
  int64_t rightRowOffset = rightTupleSet_.has_value() ? rightTupleSet_.value()->numRows() : 0;
  const auto &joiner = RecordBatchNestedLoopJoiner::make(predicate_,
                                                         outputSchema_.value(),
                                                         neededColumnIndice_);

  ::arrow::Status status;

  // read the left table a batch at a time
  ::arrow::Result<std::shared_ptr<::arrow::RecordBatch>> leftRecordBatchResult;
  ::arrow::TableBatchReader leftReader{*leftTupleSet->table()};
  leftReader.set_chunksize((int64_t) DefaultChunkSize);

  // read a left batch
  leftRecordBatchResult = leftReader.Next();
  if (!leftRecordBatchResult.ok()) {
    return tl::make_unexpected(leftRecordBatchResult.status().message());
  }
  auto leftRecordBatch = *leftRecordBatchResult;

  while (leftRecordBatch) {
    // read the right table a batch at a time
    ::arrow::Result<std::shared_ptr<::arrow::RecordBatch>> rightRecordBatchResult;
    ::arrow::TableBatchReader rightReader{*rightTupleSet->table()};
    rightReader.set_chunksize((int64_t) DefaultChunkSize);

    // read a right batch
    rightRecordBatchResult = rightReader.Next();
    if (!rightRecordBatchResult.ok()) {
      return tl::make_unexpected(rightRecordBatchResult.status().message());
    }
    auto rightRecordBatch = *rightRecordBatchResult;

    while (rightRecordBatch) {
      // join
      auto result = joiner->join(leftRecordBatch, rightRecordBatch, leftRowOffset, rightRowOffset);
      if (!result.has_value()) {
        return tl::make_unexpected(result.error());
      }

      // incremental right row offset
      rightRowOffset += rightRecordBatch->num_rows();

      // read a right batch
      rightRecordBatchResult = rightReader.Next();
      if (!rightRecordBatchResult.ok()) {
        return tl::make_unexpected(rightRecordBatchResult.status().message());
      }
      rightRecordBatch = *rightRecordBatchResult;
    }

    // incremental left row offset
    leftRowOffset += leftRecordBatch->num_rows();

    // read a left batch
    leftRecordBatchResult = leftReader.Next();
    if (!leftRecordBatchResult.ok()) {
      return tl::make_unexpected(leftRecordBatchResult.status().message());
    }
    leftRecordBatch = *leftRecordBatchResult;
  }

  // Save row match indexes
  if (leftJoinHelper_.has_value()) {
    leftJoinHelper_.value()->putRowMatchIndexes(joiner->getLeftRowMatchIndexes());
  }
  if (rightJoinHelper_.has_value()) {
    rightJoinHelper_.value()->putRowMatchIndexes(joiner->getRightRowMatchIndexes());
  }

  // Return joined result
  return joiner->toTupleSet();
}

tl::expected<void, string> NestedLoopJoinKernel::joinIncomingLeft(const shared_ptr<TupleSet> &incomingLeft) {
  // buffer tupleSet
  auto result = bufferInput(leftTupleSet_, incomingLeft);
  if (!result) {
    return tl::make_unexpected(result.error());
  }

  // create output schema and outer join helpers
  if (rightTupleSet_.has_value()) {
    bufferOutputSchema(incomingLeft, rightTupleSet_.value());
    auto makeResult = makeOuterJoinHelpers();
    if (!makeResult.has_value()) {
      return makeResult;
    }
  }
  
  // check empty
  if (!rightTupleSet_.has_value() || rightTupleSet_.value()->numRows() == 0 || incomingLeft->numRows() == 0) {
    return {};
  }

  // join
  const auto &expectedJoinedTupleSet = join(incomingLeft, rightTupleSet_.value());
  if (!expectedJoinedTupleSet.has_value()) {
    return tl::make_unexpected(expectedJoinedTupleSet.error());
  }

  // buffer join result
  auto bufferResult = buffer(expectedJoinedTupleSet.value());
  if (!bufferResult.has_value()) {
    return tl::make_unexpected(bufferResult.error());
  }

  return {};
}

tl::expected<void, string> NestedLoopJoinKernel::joinIncomingRight(const shared_ptr<TupleSet> &incomingRight) {
  // buffer tupleSet
  auto result = bufferInput(rightTupleSet_, incomingRight);
  if (!result) {
    return tl::make_unexpected(result.error());
  }

  // Create output schema and outer join helpers
  if (leftTupleSet_.has_value()) {
    bufferOutputSchema(leftTupleSet_.value(), incomingRight);
    auto makeResult = makeOuterJoinHelpers();
    if (!makeResult.has_value()) {
      return makeResult;
    }
  }

  // check empty
  if (!leftTupleSet_.has_value() || leftTupleSet_.value()->numRows() == 0 || incomingRight->numRows() == 0) {
    return {};
  }

  // join
  const auto &expectedJoinedTupleSet = join(leftTupleSet_.value(), incomingRight);
  if (!expectedJoinedTupleSet.has_value()) {
    return tl::make_unexpected(expectedJoinedTupleSet.error());
  }

  // buffer join result
  auto bufferResult = buffer(expectedJoinedTupleSet.value());
  if (!bufferResult.has_value()) {
    return tl::make_unexpected(bufferResult.error());
  }

  return {};
}

tl::expected<void, string> NestedLoopJoinKernel::buffer(const shared_ptr<TupleSet> &tupleSet) {
  if (!buffer_.has_value()) {
    buffer_ = tupleSet;
  }
  else {
    const auto &bufferedTupleSet = buffer_.value();
    const auto &concatenateResult = TupleSet::concatenate({bufferedTupleSet, tupleSet});
    if (!concatenateResult)
      return tl::make_unexpected(concatenateResult.error());

    buffer_ = concatenateResult.value();
  }

  return {};
}

const std::optional<shared_ptr<TupleSet>> &NestedLoopJoinKernel::getBuffer() const {
  return buffer_;
}

void NestedLoopJoinKernel::clearBuffer() {
  buffer_ = nullopt;
}

tl::expected<void, string> NestedLoopJoinKernel::finalize() {
  // compute outer join
  auto result = computeOuterJoin();
  if (!result) {
    return result;
  }

  return {};
}

void NestedLoopJoinKernel::bufferOutputSchema(const shared_ptr<TupleSet> &leftTupleSet, 
                                              const shared_ptr<TupleSet> &rightTupleSet) {
  if (!outputSchema_.has_value()) {
    // Create the outputSchema_ and neededColumnIndice_ from neededColumnNames_
    vector<shared_ptr<::arrow::Field>> outputFields;

    for (int c = 0; c < leftTupleSet->schema()->num_fields(); ++c) {
      auto field = leftTupleSet->schema()->field(c);
      if (neededColumnNames_.find(field->name()) != neededColumnNames_.end()) {
        neededColumnIndice_.emplace_back(make_shared<pair<bool, int>>(true, c));
        outputFields.emplace_back(field);
      }
    }
    for (int c = 0; c < rightTupleSet->schema()->num_fields(); ++c) {
      auto field = rightTupleSet->schema()->field(c);
      if (neededColumnNames_.find(field->name()) != neededColumnNames_.end()) {
        neededColumnIndice_.emplace_back(make_shared<pair<bool, int>>(false, c));
        outputFields.emplace_back(field);
      }
    }
    outputSchema_ = make_shared<::arrow::Schema>(outputFields);
  }
}

const std::optional<shared_ptr<::arrow::Schema>> &NestedLoopJoinKernel::getOutputSchema() const {
  return outputSchema_;
}

tl::expected<void, string> NestedLoopJoinKernel::makeOuterJoinHelpers() {
  if (!isOuterJoinHelperCreated_) {
    if (!outputSchema_.has_value()) {
      return tl::make_unexpected("Output schema not set when making outer join helpers");
    }
    if (isLeft_ && !leftJoinHelper_.has_value()) {
      leftJoinHelper_ = OuterJoinHelper::make(true, outputSchema_.value(), neededColumnIndice_);
    }
    if (isRight_ && !rightJoinHelper_.has_value()) {
      rightJoinHelper_ = OuterJoinHelper::make(false, outputSchema_.value(), neededColumnIndice_);
    }
    isOuterJoinHelperCreated_ = true;
  }
  return {};
}

tl::expected<void, string> NestedLoopJoinKernel::computeOuterJoin() {
  // for cartesian product, outer join = inner join
  if (!predicate_.has_value()) {
    return {};
  }

  // left outer join
  if (leftJoinHelper_.has_value() && rightTupleSet_.has_value()) {
    const auto &expLeftOutput = leftJoinHelper_.value()->compute(rightTupleSet_.value());
    if (!expLeftOutput.has_value()) {
      return tl::make_unexpected(expLeftOutput.error());
    }
    auto result = buffer(expLeftOutput.value());
    if (!result.has_value()) {
      return result;
    }
  }

  // right outer join
  if (rightJoinHelper_.has_value() && leftTupleSet_.has_value()) {
    const auto &expRightOutput = rightJoinHelper_.value()->compute(leftTupleSet_.value());
    if (!expRightOutput.has_value()) {
      return tl::make_unexpected(expRightOutput.error());
    }
    auto result = buffer(expRightOutput.value());
    if (!result.has_value()) {
      return result;
    }
  }

  return {};
}

void NestedLoopJoinKernel::clear() {
  leftTupleSet_ = nullopt;
  rightTupleSet_ = nullopt;
  clearBuffer();
  leftJoinHelper_ = nullopt;
  rightJoinHelper_ = nullopt;
}

}
