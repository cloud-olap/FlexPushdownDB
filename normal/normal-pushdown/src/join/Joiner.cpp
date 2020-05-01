//
// Created by matt on 30/4/20.
//

#include "normal/pushdown/join/Joiner.h"

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

  std::shared_ptr<Schema> joinedSchema = buildJoinedSchema();
  processProbeTuples();

  // TODO: Implement
  return tl::unexpected(std::string("Not implemented yet"));
}

std::shared_ptr<Schema> Joiner::buildJoinedSchema() const {
  auto buildSchema = hashtable_->getTupleSet()->schema();
  auto probeSchema = tuples_->schema();
  auto joinedSchema = Schema::concatenate({buildSchema, probeSchema});

  SPDLOG_DEBUG("Joined schema:\n{}", joinedSchema->showString());

  return joinedSchema;
}

void Joiner::processProbeTuples() {

  // Get the probe column
  auto expectedProbeColumn = tuples_->getColumnByName(pred_.getRightColumnName());
  if (!expectedProbeColumn.has_value()) {
	// FIXME
	//return tl::make_unexpected(expectedProbeColumn.error());
  }
  auto probeColumn = expectedProbeColumn.value();

  SPDLOG_DEBUG("Probe column:\n{}", probeColumn->showString());

  long rowCounter = 0;
  for (Column::iterator columnIterator = probeColumn->begin(); columnIterator != probeColumn->end(); columnIterator++) {
	std::shared_ptr<Scalar> probeValue = columnIterator.get();
	SPDLOG_DEBUG("Loaded probe value (probeRow: {}, probeValue: {})", rowCounter, probeValue->showString());

	auto valueIndexMapIterator = this->hashtable_->getValueRowMap()->find(probeValue);

	if (valueIndexMapIterator == this->hashtable_->getValueRowMap()->end()) {
	  SPDLOG_DEBUG("Match NOT FOUND (probeRow: {}, probeValue {})", rowCounter, probeValue->showString());
	} else {
	  auto buildRows = valueIndexMapIterator->second;
	  SPDLOG_DEBUG("Match FOUND (probeRow: {}, probeValue {}, buildRows: {})",
				   rowCounter,
				   probeValue->showString(),
				   buildRows);
	}
  }
}

