//
// Created by matt on 29/7/20.
//

#include "normal/pushdown/shuffle/ATTIC/ShuffleKernel.h"

#include <string>

#include <arrow/api.h>
#include <arrow/util/string_view.h>

using namespace normal::pushdown::shuffle;

tl::expected<std::vector<std::shared_ptr<TupleSet2>>, std::string>
ShuffleKernel::shuffle(const std::string &columnName,
					   size_t numPartitions,
					   const std::shared_ptr<TupleSet2> &tupleSet) {

  ::arrow::Result<std::shared_ptr<::arrow::RecordBatch>> recordBatchResult;
  ::arrow::Status status;

  // Get the arrow table, checking the tupleset is defined FIXME: This is dumb :(
  std::shared_ptr<::arrow::Table> table;
  if (tupleSet->getArrowTable().has_value()) {
	table = tupleSet->getArrowTable().value();
  } else {
	return tl::unexpected{fmt::format("TupleSet is undefined")};
  }

  // Get the shuffle column index, checking the column exists
  auto shuffleColumnIndex = table->schema()->GetFieldIndex(columnName);
  if (shuffleColumnIndex == -1) {
	return tl::unexpected{fmt::format("Shuffle column '{}' does not exist", columnName)};
  }

  // Define a vector of record batch vectors, one for each shuffle partition
  std::vector<std::vector<std::shared_ptr<::arrow::RecordBatch>>> shuffledRecordBatchesVector{numPartitions};

  // Read the table a batch at a time
  ::arrow::TableBatchReader reader{*table};
  reader.set_chunksize(DefaultChunkSize);
  recordBatchResult = reader.Next();
  if (!recordBatchResult.ok()) {
	return tl::unexpected{fmt::format(recordBatchResult.status().message())};
  }

  while (*recordBatchResult) {

	auto shuffleColumn = (*recordBatchResult)->column(shuffleColumnIndex);
	if (shuffleColumn->type_id() == ::arrow::StringType::type_id) {
	  auto shuffleArray = std::static_pointer_cast<::arrow::StringArray>(shuffleColumn);
	  for (int i = 0; i < shuffleArray->length(); ++i) {
		auto shuffleValue = shuffleArray->GetView(i);
		auto hash = std::hash<::arrow::util::string_view>()(shuffleValue);
		auto partitionIndex = hash % numPartitions;
		auto row = (*recordBatchResult)->Slice(i, 1);
		shuffledRecordBatchesVector[partitionIndex].emplace_back(row);
	  }
	}

	recordBatchResult = reader.Next();
	if (!recordBatchResult.ok()) {
	  return tl::unexpected{fmt::format(recordBatchResult.status().message())};
	}
  }

  // Combine the record batches into tuple sets
  std::vector<std::shared_ptr<TupleSet2>> shuffledTupleSetVector{numPartitions};
  for (size_t i = 0; i < shuffledRecordBatchesVector.size(); ++i) {
	std::shared_ptr<::arrow::Table> shuffledTable;
	auto expectedTable = ::arrow::Table::FromRecordBatches(shuffledRecordBatchesVector[i]);
	if (expectedTable.ok())
	  shuffledTupleSetVector[i] = TupleSet2::make(*expectedTable);
	else
	  return tl::unexpected{fmt::format(expectedTable.status().message())};
  }

  return shuffledTupleSetVector;
}
