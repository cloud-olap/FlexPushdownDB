//
// Created by matt on 17/6/20.
//

#include <normal/tuple/ColumnBuilder.h>
#include "normal/pushdown/shuffle/ATTIC/Shuffler.h"

using namespace normal::pushdown::shuffle;

tl::expected<std::vector<std::shared_ptr<TupleSet2>>, std::string>
Shuffler::shuffle(const std::string &columnName,
				  size_t numPartitions,
				  const std::shared_ptr<TupleSet2> &tupleSet) {

  // Get the shuffle column
  auto expectedShuffleColumn = tupleSet->getColumnByName(columnName);
  if (!expectedShuffleColumn.has_value()) {
	return tl::unexpected(expectedShuffleColumn.error());
  }
  auto shuffleColumn = expectedShuffleColumn.value();

  // Create builders for each column for each partition
  auto fields = tupleSet->schema().value()->fields();
  std::vector<std::vector<std::shared_ptr<ColumnBuilder>>> columnBuilders;
  columnBuilders.reserve(numPartitions);
  for (size_t partitionIndex = 0; partitionIndex < numPartitions; ++partitionIndex) {
	columnBuilders.emplace_back(std::vector<std::shared_ptr<ColumnBuilder>>());
	for (const auto &field: fields) {
	  columnBuilders[partitionIndex].emplace_back(ColumnBuilder::make(field->name(), field->type()));
	}
  }

  // Shuffle the tuple set
  int rowIndex = 0;
  for (const auto &columnValue: *shuffleColumn) {
	auto partitionIndex = columnValue->hash() % numPartitions;
//	SPDLOG_DEBUG("Assigning row to shuffle partition  |  row: {}, columnValue: {}, partitionIndex: {} ", rowIndex, columnValue->toString(), partitionIndex);
	for (size_t columnIndex = 0; columnIndex < fields.size(); ++columnIndex) {
	  auto column = tupleSet->getColumnByIndex(columnIndex);
	  auto scalar = column.value()->element(rowIndex);
	  auto columnBuilder = columnBuilders[partitionIndex][columnIndex];
	  columnBuilder->append(scalar.value());
	}
	++rowIndex;
  }

  // Create the vector of shuffled tuple sets
  std::vector<std::shared_ptr<TupleSet2>> shuffledTupleSets;

  // Finalize the column builders
  for (size_t partitionIndex = 0; partitionIndex < numPartitions; ++partitionIndex) {
	std::vector<std::shared_ptr<Column>> columns;
	auto columnBuilder = columnBuilders[partitionIndex];
	for (size_t columnIndex = 0; columnIndex < columnBuilder.size(); ++columnIndex) {
	  auto column = columnBuilder[columnIndex]->finalize();
	  columns.emplace_back(column);
	}

	auto shuffledTupleSet = TupleSet2::make(columns);
	shuffledTupleSets.emplace_back(shuffledTupleSet);
  }

  return shuffledTupleSets;

}
