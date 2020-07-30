//
// Created by matt on 29/7/20.
//

#include "normal/pushdown/shuffle/ShuffleKernel2.h"
#include "RecordBatchShuffler.h"

#include <string>

#include <arrow/api.h>

using namespace normal::pushdown::shuffle;

tl::expected<std::vector<std::shared_ptr<TupleSet2>>, std::string>
ShuffleKernel2::shuffle(const std::string &columnName,
						size_t numSlots,
						const std::shared_ptr<TupleSet2> &tupleSet) {

  ::arrow::Result<std::shared_ptr<::arrow::RecordBatch>> recordBatchResult;
  ::arrow::Status status;

  // Get the arrow table, checking the tupleset is defined FIXME: This is dumb :(
  std::shared_ptr<::arrow::Table> table;
  if (tupleSet->getArrowTable().has_value()) {
	table = tupleSet->getArrowTable().value();
  } else {
	return tl::make_unexpected(fmt::format("TupleSet is undefined"));
  }

  // Create the shuffler
  auto expectedShuffler = RecordBatchShuffler::make(columnName, numSlots, table->schema());
  if (!expectedShuffler.has_value()) {
	return tl::make_unexpected(expectedShuffler.error());
  }
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
	if(!expectedResult.has_value()){
	  return tl::make_unexpected(expectedResult.error());
	}

	// Read a batch
	recordBatchResult = reader.Next();
	if (!recordBatchResult.ok()) {
	  return tl::make_unexpected(recordBatchResult.status().message());
	}
	recordBatch = *recordBatchResult;
  }

  return shuffler->toTupleSets();
}
