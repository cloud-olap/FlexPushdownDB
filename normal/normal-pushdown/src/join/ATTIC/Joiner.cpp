//
// Created by matt on 30/4/20.
//

#include "normal/pushdown/join/ATTIC/Joiner.h"

#include <arrow/api.h>
//#include <arrow/table.h>

#include <normal/tuple/Column.h>
//#include <normal/pushdown/Globals.h>

#include <utility>
#include <normal/tuple/ColumnBuilder.h>

using namespace normal::pushdown::join;

Joiner::Joiner(JoinPredicate Pred,
			   std::shared_ptr<HashTable> Hashtable,
			   std::shared_ptr<normal::tuple::TupleSet2> Tuples) :
	pred_(std::move(Pred)),
	hashtable_(std::move(Hashtable)),
	tuples_(std::move(Tuples)) {}

tl::expected<std::shared_ptr<normal::tuple::TupleSet2>, std::string> Joiner::join() {

//  SPDLOG_DEBUG("Build hashtable:\n{}", hashtable_->toString());
//  SPDLOG_DEBUG("Probe tuple set:\n{}", tuples_->showString(TupleSetShowOptions(TupleSetShowOrientation::RowOriented)));

  return processProbeTuples();
}

std::shared_ptr<Schema> Joiner::buildJoinedSchema() const {
  auto buildSchema = hashtable_->getTupleSet()->schema().value();
  auto probeSchema = tuples_->schema().value();
  auto joinedSchema = Schema::concatenate({buildSchema, probeSchema});

//  SPDLOG_DEBUG("Joined schema:\n{}", joinedSchema->showString());

  return joinedSchema;
}

tl::expected<std::shared_ptr<normal::tuple::TupleSet2>, std::string> Joiner::processProbeTuples() {

  // Get the probe column
  auto expectedProbeColumn = tuples_->getColumnByName(pred_.getRightColumnName());
  if (!expectedProbeColumn.has_value()) {
	return tl::make_unexpected(expectedProbeColumn.error());
  }
  auto probeColumn = expectedProbeColumn.value();

//  SPDLOG_DEBUG("Probe column:\n{}", probeColumn->showString());

  // Create builders for each column
  auto buildFields = hashtable_->getTupleSet()->schema().value()->fields();
  auto probeFields = tuples_->schema().value()->fields();
  std::vector<std::shared_ptr<::normal::tuple::ColumnBuilder>> buildTupleSetBuilders;
  std::vector<std::shared_ptr<::normal::tuple::ColumnBuilder>> probeTupleSetBuilders;
  buildTupleSetBuilders.reserve(buildFields.size());
  for (const auto &buildField: buildFields) {
	buildTupleSetBuilders.emplace_back(ColumnBuilder::make(buildField->name(), buildField->type()));
  }
  probeTupleSetBuilders.reserve(probeFields.size());
  for (const auto &probeField: probeFields) {
	probeTupleSetBuilders.emplace_back(ColumnBuilder::make(probeField->name(), probeField->type()));
  }

  long probeRowIndex = 0;
  for (const auto& probeValue: *probeColumn) {
//	SPDLOG_DEBUG("Loaded probe value (probeRowIndex: {}, probeValue: {})", probeRowIndex, probeValue->toString());

	auto buildRowsRange = this->hashtable_->getValueRowMap()->equal_range(probeValue);

	if (std::distance(buildRowsRange.first, buildRowsRange.second) == 0) {
//	  SPDLOG_DEBUG("Match NOT FOUND (probeRowIndex: {}, probeValue {})", probeRowIndex, probeValue->toString());
	} else {
	  for(auto buildRowsIterator = buildRowsRange.first; buildRowsIterator != buildRowsRange.second; buildRowsIterator++){
	    auto buildRowIndex = buildRowsIterator->second;
//		SPDLOG_DEBUG("Match FOUND (probeRowIndex: {}, probeValue {}, buildRowIndex: {})",
//					 probeRowIndex,
//					 probeValue->toString(),
//					 buildRowIndex);

		for (size_t buildColumnIndex= 0; buildColumnIndex <buildFields.size(); ++buildColumnIndex) {
//		  auto typedBuilder = std::static_pointer_cast<::arrow::Int64Builder>(buildTupleSetBuilders.at(buildColumnIndex));
			auto builder = buildTupleSetBuilders.at(buildColumnIndex);
		  auto buildColumn = hashtable_->getTupleSet()->getColumnByIndex(buildColumnIndex);
		  auto buildValue = buildColumn.value()->element(buildRowIndex);
//		  typedBuilder->Append(probeValue2.value()->value<long>());
		  builder->append(buildValue.value());
		}

		for (size_t probeColumnIndex= 0; probeColumnIndex <probeFields.size(); ++probeColumnIndex) {
//		  auto typedBuilder = std::static_pointer_cast<::arrow::Int64Builder>(probeTupleSetBuilders.at(probeColumnIndex));
		  auto builder = probeTupleSetBuilders.at(probeColumnIndex);
		  auto probeColumn2 = tuples_->getColumnByIndex(probeColumnIndex);
		  auto probeValue2 = probeColumn2.value()->element(probeRowIndex);
//		  typedBuilder->Append(probeValue2.value()->value<long>());
		  builder->append(probeValue2.value());
		}
	  }
	}

	probeRowIndex++;
  }

  // Create the columns for the joined tuple set
//  std::vector<std::shared_ptr<::arrow::Array>> buildArrays;
//  std::vector<std::shared_ptr<::arrow::Array>> probeArrays;
//  buildArrays.reserve(buildFields.size());
//  for (const auto &buildField: buildFields) {
//	buildArrays.emplace_back(std::shared_ptr<::arrow::Array>());
//  }
//  probeArrays.reserve(probeFields.size());
//  for (const auto &probeField: probeFields) {
//	probeArrays.emplace_back(std::shared_ptr<::arrow::Array>());
//  }
//
//  // Finalize the builders
//  int buildColumnIndex = 0;
//  for (const auto &buildTupleSetBuilder: buildTupleSetBuilders) {
//	buildTupleSetBuilder->finalize(&buildArrays.at(buildColumnIndex));
//	buildColumnIndex++;
//  }
//  int probeColumnIndex = 0;
//  for (const auto &probeTupleSetBuilder: probeTupleSetBuilders) {
//	probeTupleSetBuilder->finalize(&probeArrays.at(probeColumnIndex));
//	probeColumnIndex++;
//  }

  // Create the full array of columns
//  std::vector<std::shared_ptr<::arrow::Array>> arrays;
//  arrays.insert(arrays.end(), buildArrays.begin(), buildArrays.end());
//  arrays.insert(arrays.end(), probeArrays.begin(), probeArrays.end());

  std::vector<std::shared_ptr<Column>> arrays;
  for (const auto &buildTupleSetBuilder: buildTupleSetBuilders) {
	auto array = buildTupleSetBuilder->finalize();
	arrays.push_back(array);
  }
  for (const auto &probeTupleSetBuilder: probeTupleSetBuilders) {
	auto array = probeTupleSetBuilder->finalize();
	arrays.push_back(array);
  }

  // Create the joined schema
  auto schema = buildJoinedSchema();

//  auto joinedTupleSet2 = std::make_shared<TupleSet2>(::arrow::Table::Make(schema->getSchema(), arrays));

  auto joinedTupleSet2 = TupleSet2::make(schema, arrays);

//  SPDLOG_DEBUG("Joined tuple set:\n{}", joinedTupleSet2->showString());

  return joinedTupleSet2;
}
