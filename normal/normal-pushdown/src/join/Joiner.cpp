//
// Created by matt on 30/4/20.
//

#include "normal/pushdown/join/Joiner.h"

#include <arrow/api.h>
#include <arrow/table.h>

#include <normal/tuple/Column.h>
#include <normal/pushdown/Globals.h>

#include <utility>

using namespace normal::pushdown::join;

Joiner::Joiner(JoinPredicate Pred,
			   std::shared_ptr<HashTable> Hashtable,
			   std::shared_ptr<normal::tuple::TupleSet2> Tuples) :
	pred_(std::move(Pred)),
	hashtable_(std::move(Hashtable)),
	tuples_(std::move(Tuples)) {}

tl::expected<std::shared_ptr<normal::tuple::TupleSet2>, std::string> Joiner::join() {

  SPDLOG_DEBUG("Build hashtable:\n{}", hashtable_->toString());
  SPDLOG_DEBUG("Probe tuple set:\n{}", tuples_->showString());

  return processProbeTuples();
}

std::shared_ptr<Schema> Joiner::buildJoinedSchema() const {
  auto buildSchema = hashtable_->getTupleSet()->schema();
  auto probeSchema = tuples_->schema();
  auto joinedSchema = Schema::concatenate({buildSchema, probeSchema});

  SPDLOG_DEBUG("Joined schema:\n{}", joinedSchema->showString());

  return joinedSchema;
}

tl::expected<std::shared_ptr<normal::tuple::TupleSet2>, std::string> Joiner::processProbeTuples() {

  // Get the probe column
  auto expectedProbeColumn = tuples_->getColumnByName(pred_.getRightColumnName());
  if (!expectedProbeColumn.has_value()) {
	return tl::make_unexpected(expectedProbeColumn.error());
  }
  auto probeColumn = expectedProbeColumn.value();

  SPDLOG_DEBUG("Probe column:\n{}", probeColumn->showString());

  // Create builders for each column
  auto buildFields = hashtable_->getTupleSet()->schema()->fields();
  auto probeFields = tuples_->schema()->fields();
  std::vector<std::shared_ptr<::arrow::ArrayBuilder>> buildTupleSetBuilders;
  std::vector<std::shared_ptr<::arrow::ArrayBuilder>> probeTupleSetBuilders;
  buildTupleSetBuilders.reserve(buildFields.size());
  for (const auto &buildField: buildFields) {
	buildTupleSetBuilders.emplace_back(std::make_shared<::arrow::Int64Builder>());
  }
  probeTupleSetBuilders.reserve(probeFields.size());
  for (const auto &probeField: probeFields) {
	probeTupleSetBuilders.emplace_back(std::make_shared<::arrow::Int64Builder>());
  }

  long probeRowIndex = 0;
  for (const auto& probeValue: *probeColumn) {
	SPDLOG_DEBUG("Loaded probe value (probeRowIndex: {}, probeValue: {})", probeRowIndex, probeValue->showString());

	auto buildRowsRange = this->hashtable_->getValueRowMap()->equal_range(probeValue);

	if (std::distance(buildRowsRange.first, buildRowsRange.second) == 0) {
	  SPDLOG_DEBUG("Match NOT FOUND (probeRowIndex: {}, probeValue {})", probeRowIndex, probeValue->showString());
	} else {
	  for(auto buildRowsIterator = buildRowsRange.first; buildRowsIterator != buildRowsRange.second; buildRowsIterator++){
	    auto buildRowIndex = buildRowsIterator->second;
		SPDLOG_DEBUG("Match FOUND (probeRowIndex: {}, probeValue {}, buildRowIndex: {})",
					 probeRowIndex,
					 probeValue->showString(),
					 buildRowIndex);

		int buildColumnIndex = 0;
		for (const auto &buildField: buildFields) {
		  auto typedBuilder = std::static_pointer_cast<::arrow::Int64Builder>(buildTupleSetBuilders.at(buildColumnIndex));
		  auto buildColumn = hashtable_->getTupleSet()->getColumnByIndex(buildColumnIndex);
		  auto buildValue = buildColumn.value()->value(buildRowIndex);
		  typedBuilder->Append(buildValue.value()->value<long>());
		  buildColumnIndex++;
		}

		int probeColumnIndex = 0;
		for (const auto &probeField: probeFields) {
		  auto typedBuilder = std::static_pointer_cast<::arrow::Int64Builder>(probeTupleSetBuilders.at(probeColumnIndex));
		  auto probeColumn2 = tuples_->getColumnByIndex(probeColumnIndex);
		  auto probeValue2 = probeColumn2.value()->value(probeRowIndex);
		  typedBuilder->Append(probeValue2.value()->value<long>());
		  probeColumnIndex++;
		}
	  }
	}

	probeRowIndex++;
  }

  // Create the columns for the joined tuple set
  std::vector<std::shared_ptr<::arrow::Array>> buildArrays;
  std::vector<std::shared_ptr<::arrow::Array>> probeArrays;
  buildArrays.reserve(buildFields.size());
  for (const auto &buildField: buildFields) {
	buildArrays.emplace_back(std::shared_ptr<::arrow::Array>());
  }
  probeArrays.reserve(probeFields.size());
  for (const auto &probeField: probeFields) {
	probeArrays.emplace_back(std::shared_ptr<::arrow::Array>());
  }

  // Finalize the builders
  int buildColumnIndex = 0;
  for (const auto &buildTupleSetBuilder: buildTupleSetBuilders) {
	buildTupleSetBuilder->Finish(&buildArrays.at(buildColumnIndex));
	buildColumnIndex++;
  }
  int probeColumnIndex = 0;
  for (const auto &probeTupleSetBuilder: probeTupleSetBuilders) {
	probeTupleSetBuilder->Finish(&probeArrays.at(probeColumnIndex));
	probeColumnIndex++;
  }

  // Create the full array of columns
  std::vector<std::shared_ptr<::arrow::Array>> arrays;
  arrays.insert(arrays.end(), buildArrays.begin(), buildArrays.end());
  arrays.insert(arrays.end(), probeArrays.begin(), probeArrays.end());

  // Create the joined schema
  auto schema = buildJoinedSchema();

  auto joinedTupleSet2 = std::make_shared<TupleSet2>(::arrow::Table::Make(schema->getSchema(), arrays));

  SPDLOG_DEBUG("Joined tuple set:\n{}", joinedTupleSet2->showString());

  return joinedTupleSet2;
}
