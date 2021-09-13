//
// Created by Han Cao on 5/20/21.
//


#include <normal/tuple/ArrayAppenderWrapper.h>
#include <normal/pushdown/join/HashJoinProbe.h>
#include <antijoin/RecordBatchAntiJoiner.h>

using namespace normal::pushdown::antijoin;

RecordBatchAntiJoiner::RecordBatchAntiJoiner(std::shared_ptr<TupleSetIndex> buildTupleSetIndex,
                                             std::string probeJoinColumnName,
                                             std::shared_ptr<::arrow::Schema> outputSchema,
                                             std::vector<std::shared_ptr<std::pair<bool, int>>> neededColumnIndice) :
        buildTupleSetIndex_(std::move(buildTupleSetIndex)),
        probeJoinColumnName_(std::move(probeJoinColumnName)),
        outputSchema_(std::move(outputSchema)),
        neededColumnIndice_(std::move(neededColumnIndice)),
        joinedArrayVectors_{static_cast<size_t>(outputSchema_->num_fields())} {
}

tl::expected <std::shared_ptr<RecordBatchAntiJoiner>, std::string>
RecordBatchAntiJoiner::make(const std::shared_ptr<TupleSetIndex> &buildTupleSetIndex,
                            const std::string &probeJoinColumnName,
                            const std::shared_ptr<::arrow::Schema> &outputSchema,
                            const std::vector<std::shared_ptr<std::pair<bool, int>>> &neededColumnIndice) {
    auto canonicalColumnName = ColumnName::canonicalize(probeJoinColumnName);
//    SPDLOG_CRITICAL(outputSchema->ToString());
    return std::make_shared<RecordBatchAntiJoiner>(buildTupleSetIndex, canonicalColumnName, outputSchema,
                                                   neededColumnIndice);
}

tl::expected<void, std::string>
RecordBatchAntiJoiner::antijoin(const std::shared_ptr<::arrow::RecordBatch> &recordBatch) {

    arrow::Status status;

    // Combine the chunks in the build table so we have single arrays for each column
    auto combineResult = buildTupleSetIndex_->combine();
    if (!combineResult)
        return tl::make_unexpected(combineResult.error());

    //  buildTupleSetIndex_->validate();

    auto buildTable = buildTupleSetIndex_->getTable();  // log table

    // Get an reference to the probe array to join on
    const auto &probeJoinColumn = recordBatch->GetColumnByName(probeJoinColumnName_);  // OG data

    // FIXME: The bug occurs here


    // Create a finder for the array index
    auto expectedIndexFinder = ArraySetIndexFinderBuilder::make(buildTupleSetIndex_, probeJoinColumn);
    if (!expectedIndexFinder.has_value())
        return tl::make_unexpected(expectedIndexFinder.error());
    auto indexFinder = expectedIndexFinder.value();

    // Create references to each array in the index
    ::arrow::ArrayVector buildColumns;
    for (const auto &column: buildTable->columns()) {
        buildColumns.emplace_back(column->chunk(0));
    }

    // Create references to each array in the record batch
    std::vector<std::shared_ptr<::arrow::Array>> probeColumns{static_cast<size_t>(recordBatch->num_columns())};
    for (int c = 0; c < recordBatch->num_columns(); ++c) {
        probeColumns[c] = recordBatch->column(c);
    }

    // create appenders to create the destination arrays
    std::vector<std::shared_ptr<ArrayAppender>> appenders{static_cast<size_t>(outputSchema_->num_fields())};

    // construct the arrayAppenders
    for (int c = 0; c < outputSchema_->num_fields(); ++c) {
        auto expectedAppender = ArrayAppenderBuilder::make(outputSchema_->field(c)->type(), 0);
        if (!expectedAppender.has_value())
            return tl::make_unexpected(expectedAppender.error());
        appenders[c] = expectedAppender.value();
    }

    // loop thru the probe table
    for (int64_t pr = 0; pr < probeJoinColumn->length(); ++pr) {
        std::vector<int64_t> buildRows = indexFinder->find(pr);  // maybe use a not in is better?

        // if the key in probe table is not in the build table
        if (buildRows.empty()) {  // not in the log file
            // add this row to the appender
            // do it column by column
            for (size_t c = 0; c < neededColumnIndice_.size(); ++c) {
                auto appendResult = appenders[c]->safeAppendValue(probeColumns[neededColumnIndice_[c]->second], pr);
                if (!appendResult) return appendResult;  // why??
            }
        } else {  // this means we have a corresponding row in the log file
            // we do not add the OG record, and we add the log record

            // we know that there is could be only one match, but we still loop thru
            // TODO: at here maybe we do not have to do the log compression, we just make sure we add the last result of find
            for (const auto br : buildRows) {
                for (size_t c = 0; c < neededColumnIndice_.size(); ++c) {
                    auto appendResult = appenders[c]->safeAppendValue(buildColumns[neededColumnIndice_[c]->second], br);
                    if (!appendResult) return appendResult;  // why??
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

//    SPDLOG_CRITICAL(neededColumnIndice_.size());
    return {};
}

tl::expected <std::shared_ptr<TupleSet2>, std::string>
RecordBatchAntiJoiner::toTupleSet() {
    arrow::Status status;

    // Make chunked arrays
    std::vector<std::shared_ptr<::arrow::ChunkedArray>> chunkedArrays;
    for (const auto &joinedArrayVector: joinedArrayVectors_) {
        // check empty
        if (joinedArrayVector.empty()) {
            return TupleSet2::make(Schema::make(outputSchema_));
        }

        auto chunkedArray = std::make_shared<::arrow::ChunkedArray>(joinedArrayVector);
        chunkedArrays.emplace_back(chunkedArray);
    }

    auto joinedTable = ::arrow::Table::Make(outputSchema_, chunkedArrays);
    auto joinedTupleSet = TupleSet2::make(joinedTable);

    joinedArrayVectors_.clear();

    return joinedTupleSet;
}

