//
// Created by matt on 29/7/20.
//

#include "normal/pushdown/shuffle/ShuffleKernel2.h"
#include "normal/pushdown/shuffle/RecordBatchShuffler2.h"

#include <string>

#include <arrow/api.h>

using namespace normal::pushdown::shuffle;

tl::expected<std::vector<std::shared_ptr<TupleSet2>>, std::string>
ShuffleKernel2::shuffle(const std::string &columnName,
						size_t numSlots,
						const TupleSet2 &tupleSet) {

  ::arrow::Result<std::shared_ptr<::arrow::RecordBatch>> recordBatchResult;
  ::arrow::Status status;

  // Get the arrow table, checking the tupleset is defined FIXME: This is dumb :(
  if (!tupleSet.getArrowTable().has_value())
	return tl::make_unexpected(fmt::format("TupleSet is undefined"));
  auto table = tupleSet.getArrowTable().value();

  // Create the shuffler
  auto expectedShuffler = RecordBatchShuffler2::make(columnName, numSlots, table->schema(), table->num_rows());
  if (!expectedShuffler.has_value())
	return tl::make_unexpected(expectedShuffler.error());
  auto shuffler = expectedShuffler.value();

  // Read the table a batch at a time
  ::arrow::TableBatchReader reader{*table};
  reader.set_chunksize(DefaultChunkSize);

  // Read a batch
  recordBatchResult = reader.Next();
  if (!recordBatchResult.ok()) {
	return tl::make_unexpected(recordBatchResult.status().message());
  }
  auto recordBatch = *recordBatchResult;

  while (recordBatch) {

	// Shuffle batch
	auto expectedResult = shuffler->shuffle(recordBatch);
	if(!expectedResult.has_value())
	  return tl::make_unexpected(expectedResult.error());

	// Read a batch
	recordBatchResult = reader.Next();
	if (!recordBatchResult.ok())
	  return tl::make_unexpected(recordBatchResult.status().message());
	recordBatch = *recordBatchResult;
  }

  auto expectedShuffledTupleSets = shuffler->toTupleSets();

#ifndef NDEBUG

  assert(expectedShuffledTupleSets.has_value());

  size_t totalNumRows = 0;
  for(const auto &shuffledTupleSet: expectedShuffledTupleSets.value()){
	assert(shuffledTupleSet->getArrowTable().has_value());
	assert(shuffledTupleSet->getArrowTable().value()->ValidateFull().ok());
	totalNumRows += shuffledTupleSet->numRows();
  }

  assert(totalNumRows == static_cast<size_t>(tupleSet.numRows()));

#endif

	return expectedShuffledTupleSets;
}
