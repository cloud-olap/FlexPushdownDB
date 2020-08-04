//
// Created by matt on 3/8/20.
//

#include "RecordBatchJoiner.h"

using namespace normal::pushdown::join;

RecordBatchJoiner::RecordBatchJoiner(std::shared_ptr<TupleSetIndex> buildTupleSetIndex,
									 std::string probeJoinColumnName,
									 std::shared_ptr<::arrow::Schema> outputSchema) :
	buildTupleSetIndex_(std::move(buildTupleSetIndex)),
	probeJoinColumnName_(std::move(probeJoinColumnName)),
	outputSchema_(std::move(outputSchema)),
	joinedArrayVectors_{static_cast<size_t>(outputSchema_->num_fields())} {
}

tl::expected<std::shared_ptr<RecordBatchJoiner>, std::string>
RecordBatchJoiner::make(const std::shared_ptr<TupleSetIndex> &buildTupleSetIndex,
						const std::string &probeJoinColumnName,
						const std::shared_ptr<::arrow::Schema> &outputSchema) {
  auto canonicalColumnName = ColumnName::canonicalize(probeJoinColumnName);
  return std::make_shared<RecordBatchJoiner>(buildTupleSetIndex, canonicalColumnName, outputSchema);
}

tl::expected<void, std::string>
RecordBatchJoiner::join(const std::shared_ptr<::arrow::RecordBatch> &recordBatch) {

  arrow::Status status;

  // Get an reference to the probe array to join on
  const auto &probeJoinColumn = recordBatch->GetColumnByName(probeJoinColumnName_);

  // Create a finder for the array index
  auto expectedIndexFinder = ArraySetIndexFinderBuilder::make(buildTupleSetIndex_, probeJoinColumn);
  if (!expectedIndexFinder.has_value())
	return tl::make_unexpected(expectedIndexFinder.error());
  auto indexFinder = expectedIndexFinder.value();

  // Combine the chunks in the build table so we have single arrays for each column
  std::shared_ptr<::arrow::Table> buildTable;
  status = buildTupleSetIndex_->getTable()->CombineChunks(::arrow::default_memory_pool(), &buildTable);
  if (!status.ok())
	return tl::make_unexpected(status.message());

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
		buildAppenders[c]->appendValue(buildColumns[c], br);
	  }

	  // Iterate the probe columns
	  for (size_t c = 0; c < probeColumns.size(); ++c) {

		// Append each row from the probe column
		probeAppenders[c]->appendValue(probeColumns[c], pr);
	  }
	}
  }

  // Create arrays from the appenders
  for (size_t c=0; c<buildAppenders.size();++c) {
	auto expectedArray = buildAppenders[c]->finalize();
	if (!expectedArray.has_value())
	  return tl::make_unexpected(status.message());
	else
	  joinedArrayVectors_[c].emplace_back(expectedArray.value());
  }

  for (size_t c=0; c<probeAppenders.size();++c) {
	auto expectedArray = probeAppenders[c]->finalize();
	if (!expectedArray.has_value())
	  return tl::make_unexpected(status.message());
	else
	  joinedArrayVectors_[buildAppenders.size() + c].emplace_back(expectedArray.value());
  }

  return {};
}

tl::expected<std::shared_ptr<TupleSet2>, std::string>
RecordBatchJoiner::toTupleSet() {
  arrow::Status status;

  // Make chunked arrays
  std::vector<std::shared_ptr<::arrow::ChunkedArray>> chunkedArrays;
  for(const auto &joinedArrayVector: joinedArrayVectors_){
    auto chunkedArray = std::make_shared<::arrow::ChunkedArray>(joinedArrayVector);
	chunkedArrays.emplace_back(chunkedArray);
  }

  auto joinedTable = ::arrow::Table::Make (outputSchema_, chunkedArrays);
  auto joinedTupleSet = TupleSet2::make(joinedTable);

  return joinedTupleSet;
}
