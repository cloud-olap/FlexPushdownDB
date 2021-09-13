//
// Created by Han Cao on 5/19/21.
//

#include "normal/pushdown/join/HashJoinProbeKernel2.h"

#include <utility>

#include <arrow/api.h>
#include <antijoin/HashAntiJoinProbeKernel.h>
#include <antijoin/RecordBatchAntiJoiner.h>

#include "normal/pushdown/join/RecordBatchJoiner.h"


using namespace normal::pushdown::antijoin;

HashAntiJoinProbeKernel::HashAntiJoinProbeKernel(join::JoinPredicate pred, std::set<std::string> neededColumnNames) :
        pred_(std::move(pred)), neededColumnNames_(std::move(neededColumnNames)) {}

HashAntiJoinProbeKernel
HashAntiJoinProbeKernel::make(join::JoinPredicate pred, std::set<std::string> neededColumnNames) {
    return antijoin::HashAntiJoinProbeKernel(std::move(pred), std::move(neededColumnNames));
}

tl::expected<void, std::string>
HashAntiJoinProbeKernel::putBuildTupleSetIndex(const std::shared_ptr<TupleSetIndex> &tupleSetIndex) {

    if (tupleSetIndex->getTable()->schema()->GetFieldIndex(pred_.getLeftColumnName()) == -1)
        return tl::make_unexpected(fmt::format(
                "Cannot put build tuple set index into probe kernel. Index does not contain join predicate left column '{}'",
                pred_.getLeftColumnName()));

    if (!buildTupleSetIndex_.has_value()) {
        buildTupleSetIndex_ = tupleSetIndex;
        return {};
    }
    return buildTupleSetIndex_.value()->merge(tupleSetIndex);
}

tl::expected<void, std::string> HashAntiJoinProbeKernel::putProbeTupleSet(const std::shared_ptr<TupleSet2> &tupleSet) {

    if (tupleSet->getArrowTable().value()->schema()->GetFieldIndex(pred_.getRightColumnName()) == -1)
        return tl::make_unexpected(fmt::format(
                "Cannot put probe tuple set into probe kernel. Tuple set does not contain join predicate right column '{}'",
                pred_.getRightColumnName()));

    if (!probeTupleSet_.has_value()) {
        probeTupleSet_ = tupleSet;
        return {};
    }
    return probeTupleSet_.value()->append(tupleSet);
}

tl::expected <std::shared_ptr<normal::tuple::TupleSet2>, std::string>
join1(const std::shared_ptr<RecordBatchAntiJoiner> &joiner, const std::shared_ptr<TupleSet2> &tupleSet) {
    ::arrow::Result<std::shared_ptr<::arrow::RecordBatch>> recordBatchResult;
    ::arrow::Status status;

    // Read the table a batch at a time
    auto probeTable = tupleSet->getArrowTable().value();
    ::arrow::TableBatchReader reader{*probeTable};
    reader.set_chunksize(DefaultChunkSize);

    // Read a batch
    recordBatchResult = reader.Next();
    if (!recordBatchResult.ok()) {
        return tl::make_unexpected(recordBatchResult.status().message());
    }
    auto recordBatch = *recordBatchResult;

    while (recordBatch) {

        // join
        auto result = joiner->antijoin(recordBatch);
        if (!result.has_value()) {
            return tl::make_unexpected(result.error());
        }

        // Read a batch
        recordBatchResult = reader.Next();
        if (!recordBatchResult.ok()) {
            return tl::make_unexpected(recordBatchResult.status().message());
        }
        recordBatch = *recordBatchResult;
    }

    // Get joined result
    auto expectedJoinedTupleSet = joiner->toTupleSet();
//    SPDLOG_CRITICAL(expectedJoinedTupleSet.value()->numRows());


#ifndef NDEBUG

    assert(expectedJoinedTupleSet.has_value());
    assert(expectedJoinedTupleSet.value()->getArrowTable().has_value());
    auto result = expectedJoinedTupleSet.value()->getArrowTable().value()->ValidateFull();
    if (!result.ok())
        return tl::make_unexpected(fmt::format("{}, HashJoinProbeKernel2", result.message()));

#endif

    return expectedJoinedTupleSet.value();
}

tl::expected<void, std::string>
HashAntiJoinProbeKernel::joinBuildTupleSetIndex(const std::shared_ptr<TupleSetIndex> &tupleSetIndex) {

    // Buffer tupleSet if having tuples
    if (tupleSetIndex->size() > 0) {
        auto result = putBuildTupleSetIndex(tupleSetIndex);
        if (!result)
            return tl::make_unexpected(result.error());
    }



    // Check empty
    if (!probeTupleSet_.has_value() || probeTupleSet_.value()->numRows() == 0 || tupleSetIndex->size() == 0) {
        return {};
    }

    // Create output schema
    bufferOutputSchema(tupleSetIndex, probeTupleSet_.value());

    // Create joiner
    auto expectedJoiner = RecordBatchAntiJoiner::make(tupleSetIndex, pred_.getRightColumnName(), outputSchema_.value(),
                                                      neededColumnIndice_);

    if (!expectedJoiner.has_value()) {
        return tl::make_unexpected(expectedJoiner.error());
    }

    // AntiJoin
    auto expectedJoinedTupleSet = join1(expectedJoiner.value(), probeTupleSet_.value());
    if (!expectedJoinedTupleSet.has_value())
        return tl::make_unexpected(expectedJoinedTupleSet.error());

    // Buffer join result
    auto bufferResult = buffer(expectedJoinedTupleSet.value());
    if (!bufferResult.has_value())
        return tl::make_unexpected(bufferResult.error());

    return {};
}

tl::expected<void, std::string> HashAntiJoinProbeKernel::joinProbeTupleSet(const std::shared_ptr<TupleSet2> &tupleSet) {

    // Buffer tupleSet if having tuples
    if (tupleSet->numRows() > 0) {
        auto result = putProbeTupleSet(tupleSet);
        if (!result)
            return tl::make_unexpected(result.error());
    }

    // Check empty
    if (!buildTupleSetIndex_.has_value() || buildTupleSetIndex_.value()->size() == 0 || tupleSet->numRows() == 0) {
        return {};
    }

    // Create output schema
    bufferOutputSchema(buildTupleSetIndex_.value(), tupleSet);

    // Create joiner
    auto expectedJoiner = RecordBatchAntiJoiner::make(buildTupleSetIndex_.value(), pred_.getRightColumnName(),
                                                      outputSchema_.value(), neededColumnIndice_);
    if (!expectedJoiner.has_value()) {
        return tl::make_unexpected(expectedJoiner.error());
    }

    // Join
    auto expectedJoinedTupleSet = join1(expectedJoiner.value(), tupleSet);
    if (!expectedJoinedTupleSet.has_value())
        return tl::make_unexpected(expectedJoinedTupleSet.error());

    // Buffer join result
    auto bufferResult = buffer(expectedJoinedTupleSet.value());
    if (!bufferResult.has_value())
        return tl::make_unexpected(bufferResult.error());

    return {};
}

tl::expected<void, std::string> HashAntiJoinProbeKernel::buffer(const std::shared_ptr<TupleSet2> &tupleSet) {

    if (!buffer_.has_value()) {
        buffer_ = tupleSet;
    } else {
        const auto &bufferedTupleSet = buffer_.value();
        const auto &concatenateResult = TupleSet2::concatenate({bufferedTupleSet, tupleSet});
        if (!concatenateResult)
            return tl::make_unexpected(concatenateResult.error());

        buffer_ = concatenateResult.value();
    }

    return {};
}

const std::optional<std::shared_ptr<normal::tuple::TupleSet2>> &HashAntiJoinProbeKernel::getBuffer() const {
    return buffer_;
}

void HashAntiJoinProbeKernel::clear() {
    buffer_ = std::nullopt;
}

void HashAntiJoinProbeKernel::bufferOutputSchema(const std::shared_ptr<TupleSetIndex> &tupleSetIndex,
                                                 const std::shared_ptr<TupleSet2> &tupleSet) {
    if (!outputSchema_.has_value()) {

        // Create the outputSchema_ and neededColumnIndice_ from neededColumnNames_
        std::vector<std::shared_ptr<::arrow::Field>> outputFields;

        for (int c = 0; c < tupleSetIndex->getTable()->schema()->num_fields(); ++c) {
            auto field = tupleSetIndex->getTable()->schema()->field(c);
            if (neededColumnNames_.find(field->name()) != neededColumnNames_.end()) {
                neededColumnIndice_.emplace_back(std::make_shared<std::pair<bool, int>>(true, c));
                outputFields.emplace_back(field);
            }
        }

        outputSchema_ = std::make_shared<::arrow::Schema>(outputFields);

    }
}
