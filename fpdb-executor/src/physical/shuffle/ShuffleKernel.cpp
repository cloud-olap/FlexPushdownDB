//
// Created by matt on 29/7/20.
//

#include <fpdb/executor/physical/shuffle/ShuffleKernel.h>
#include <fpdb/executor/physical/shuffle/RecordBatchShuffler.h>
#include <arrow/api.h>
#include <string>

using namespace fpdb::executor::physical::shuffle;

tl::expected<vector<shared_ptr<TupleSet>>, string>
ShuffleKernel::shuffle(const vector<string> &columnNames,
                        size_t numSlots,
                        const TupleSet &tupleSet) {
  // handle special case
  if (numSlots == 1) {
    return vector<shared_ptr<TupleSet>>{make_shared<TupleSet>(tupleSet)};
  }

  ::arrow::Result<shared_ptr<::arrow::RecordBatch>> recordBatchResult;
  ::arrow::Status status;

  // Get the arrow table, checking the tupleset is defined FIXME: This is dumb :(
  if (!tupleSet.valid())
	return tl::make_unexpected(fmt::format("TupleSet is undefined"));
  auto table = tupleSet.table();

  // Create the shuffler
  auto expectedShuffler = RecordBatchShuffler::make(columnNames, numSlots, table->schema(), table->num_rows());
  if (!expectedShuffler.has_value())
	return tl::make_unexpected(expectedShuffler.error());
  auto shuffler = expectedShuffler.value();

  // Read the table a batch at a time
  ::arrow::TableBatchReader reader{*table};
  reader.set_chunksize((int64_t) DefaultChunkSize);

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
	assert(shuffledTupleSet->valid());
	assert(shuffledTupleSet->validate());
	totalNumRows += shuffledTupleSet->numRows();
  }

  assert(totalNumRows == static_cast<size_t>(tupleSet.numRows()));

#endif

	return expectedShuffledTupleSets;
}
