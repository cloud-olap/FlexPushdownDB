//
// Created by matt on 3/8/20.
//

#ifndef NORMAL_NORMAL_PUSHDOWN_INCLUDE_NORMAL_PUSHDOWN_JOIN_RECORDBATCHJOINER_H
#define NORMAL_NORMAL_PUSHDOWN_INCLUDE_NORMAL_PUSHDOWN_JOIN_RECORDBATCHJOINER_H

#include <memory>
#include <utility>

#include <normal/pushdown/join/TypedArraySetIndex.h>
#include <normal/pushdown/join/ArraySetIndexFinder.h>
#include <normal/pushdown/shuffle/ArrayAppender.h>
#include <normal/tuple/ColumnName.h>
#include <normal/tuple/TupleSet2.h>
#include "ArraySetIndexFinderWrapper.h"

using namespace normal::pushdown::shuffle;

class RecordBatchJoiner {
public:

  RecordBatchJoiner(std::shared_ptr<ArraySetIndex> BuildArraySetIndex,
					std::string JoinColumnName,
					std::shared_ptr<::arrow::Schema> OutputSchema)
	  : buildArraySetIndex_(std::move(BuildArraySetIndex)), joinColumnName_(std::move(JoinColumnName)),
		outputSchema_(std::move(OutputSchema)),
		joinedArrayVector_{} {
	joinedArrayVector_.reserve(static_cast<size_t>(outputSchema_->num_fields()));
  }

  static tl::expected<std::shared_ptr<RecordBatchJoiner>,
					  std::string> make(const std::shared_ptr<ArraySetIndex> &BuildArraySetIndex,
										const std::string &columnName,
										const std::shared_ptr<::arrow::Schema> &OutputSchema) {
	auto canonicalColumnName = ColumnName::canonicalize(columnName);
	return std::make_shared<RecordBatchJoiner>(BuildArraySetIndex, columnName, OutputSchema);
  }

  tl::expected<void, std::string> join(const std::shared_ptr<::arrow::RecordBatch> &recordBatch) {

	arrow::Status status;

	// Get an reference to the probe array to join on
	const auto &probeJoinColumn = recordBatch->GetColumnByName(joinColumnName_);

	// Create a finder for the array index
	auto expectedIndexFinder = ArraySetIndexFinderBuilder::make(buildArraySetIndex_, probeJoinColumn);
	if (!expectedIndexFinder.has_value())
	  return tl::make_unexpected(expectedIndexFinder.error());
	auto indexFinder = expectedIndexFinder.value();

	// Create references to each array in the index
	std::vector<std::shared_ptr<::arrow::ChunkedArray>> buildColumns = buildArraySetIndex_->columns();

	// Create references to each array in the record batch
	std::vector<std::shared_ptr<::arrow::Array>> probeColumns{static_cast<size_t>(recordBatch->num_columns())};
	for (int c = 0; c < recordBatch->num_columns(); ++c) {
	  probeColumns[c] = recordBatch->column(c);
	}

	// Create appenders to create the destination arrays
	std::vector<std::shared_ptr<ArrayAppender>> buildAppenders{buildColumns.size()};
	for (size_t c = 0; c < buildColumns.size(); ++c) {
	  auto expectedAppender = ArrayAppender::make(buildColumns[c]->type(), 0);
	  if (!expectedAppender.has_value())
		return tl::make_unexpected(expectedAppender.error());
	  buildAppenders[c] = expectedAppender.value();
	}

	std::vector<std::shared_ptr<ArrayAppender>> probeAppenders{probeColumns.size()};
	for (size_t c = 0; c < probeColumns.size(); ++c) {
	  auto expectedAppender = ArrayAppender::make(probeColumns[c]->type(), 0);
	  if (!expectedAppender.has_value())
		return tl::make_unexpected(expectedAppender.error());
	  probeAppenders[c] = expectedAppender.value();
	}

	// Iterate through the probe join column
	for (int64_t pr = 0; pr < probeJoinColumn->length(); ++pr) {

	  // Find matched rows in the build column
	  std::vector<int64_t> buildRows = indexFinder->find(pr);

	  // Iterate the matched rows in the build column
	  for (const auto br: buildRows) {

		// Iterate the build columns
		for (size_t c = 0; c < buildColumns.size(); ++c) {

		  // Append each row from the build column
		  buildAppenders[c]->appendValue(buildColumns[c]->Slice(br, br + 1)->chunk(0), 0);
		}

		// Iterate the probe columns
		for (size_t c = 0; c < probeColumns.size(); ++c) {

		  // Append each row from the probe column
		  probeAppenders[c]->appendValue(probeColumns[c], pr);
		}
	  }
	}

	for (auto & probeAppender : probeAppenders) {
	  auto expectedArray = probeAppender->finalize();
	  if (!expectedArray.has_value())
		return tl::make_unexpected(status.message());
	  else
		joinedArrayVector_.emplace_back(expectedArray.value());
	}

	// Create arrays from the appenders
	for (auto & buildAppender : buildAppenders) {
	  auto expectedArray = buildAppender->finalize();
	  if (!expectedArray.has_value())
		return tl::make_unexpected(status.message());
	  else
		joinedArrayVector_.emplace_back(expectedArray.value());
	}

	return {};
  }

  tl::expected<std::shared_ptr<TupleSet2>, std::string> toTupleSet() {
	arrow::Status status;

	auto joinedTable = ::arrow::Table::Make (outputSchema_, joinedArrayVector_);
	auto joinedTupleSet = TupleSet2::make(joinedTable);

	return joinedTupleSet;
  }

private:
  std::shared_ptr<ArraySetIndex> buildArraySetIndex_;
  std::string joinColumnName_;
  std::shared_ptr<::arrow::Schema> outputSchema_;
  std::vector<std::shared_ptr<::arrow::Array>> joinedArrayVector_;

};

#endif //NORMAL_NORMAL_PUSHDOWN_INCLUDE_NORMAL_PUSHDOWN_JOIN_RECORDBATCHJOINER_H
