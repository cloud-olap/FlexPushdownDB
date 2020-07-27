//
// Created by matt on 8/7/20.
//

#include "normal/pushdown/cache/CacheLoad.h"

#include <normal/pushdown/cache/CacheHelper.h>
#include <normal/pushdown/TupleMessage.h>
#include <normal/pushdown/scan/ScanMessage.h>

#include <utility>

using namespace normal::pushdown::cache;
using namespace normal::core::message;
using namespace normal::pushdown::scan;

CacheLoad::CacheLoad(std::string name,
					 std::vector<std::string> ColumnNames,
					 std::shared_ptr<Partition> Partition,
					 int64_t StartOffset,
					 int64_t FinishOffset) : Operator(std::move(name), "CacheLoad"),
											 columnNames_(std::move(ColumnNames)), partition_(std::move(Partition)),
											 startOffset_(StartOffset), finishOffset_(FinishOffset) {

}

std::shared_ptr<CacheLoad> CacheLoad::make(const std::string &name,
										   const std::vector<std::string> &columnNames,
										   const std::shared_ptr<Partition> &partition,
										   int64_t startOffset,
										   int64_t finishOffset) {

  std::vector<std::string> canonicalColumnNames;
  std::transform(columnNames.begin(), columnNames.end(),
				 std::back_inserter(canonicalColumnNames),
				 [](auto name) -> auto { return ColumnName::canonicalize(name); });

  return std::make_shared<CacheLoad>(name,
									 canonicalColumnNames,
									 partition,
									 startOffset,
									 finishOffset);
}

void CacheLoad::onReceive(const Envelope &message) {
  if (message.message().type() == "StartMessage") {
	this->onStart();
  } else if (message.message().type() == "LoadResponseMessage") {
	auto loadResponseMessage = dynamic_cast<const LoadResponseMessage &>(message.message());
	this->onCacheLoadResponse(loadResponseMessage);
  } else {
	// FIXME: Propagate error properly
	throw std::runtime_error("Unrecognized message type " + message.message().type());
  }
}

void CacheLoad::onStart() {

  if (!hitOperator_)
	throw std::runtime_error("Hit consumer not set ");

  if (!missOperator_)
	throw std::runtime_error("Miss consumer not set");

  SPDLOG_DEBUG("Starting operator  |  name: '{}', hitOperator: '{}', missOperator: '{}'",
			   this->name(),
			   hitOperator_->name(),
			   missOperator_->name());
  requestLoadSegmentsFromCache();
}

void CacheLoad::requestLoadSegmentsFromCache() {
  CacheHelper::requestLoadSegmentsFromCache(columnNames_, partition_, startOffset_, finishOffset_, name(), ctx());
}

void CacheLoad::onCacheLoadResponse(const LoadResponseMessage &Message) {

  std::vector<std::string> missedSegmentColumnNames;
  std::vector<std::shared_ptr<Column>> hitSegmentColumns;

  auto hitSegments = Message.getSegments();

  SPDLOG_DEBUG("Loaded segments from cache  |  numRequested: {}, numHit: {}", columnNames_.size(), hitSegments.size());

  // Gather the hit segment columns and missed segment columns
  for (const auto &columnName: columnNames_) {
	auto segmentKey = SegmentKey::make(partition_, columnName, SegmentRange::make(startOffset_, finishOffset_));

	auto segment = hitSegments.find(segmentKey);
	if (segment != hitSegments.end()) {
	  hitSegmentColumns.push_back(segment->second->getColumn());
	} else {
	  missedSegmentColumnNames.push_back(columnName);
	}
  }

  // Create a tuple set from the hit segments
  auto hitTupleSet = TupleSet2::make(hitSegmentColumns);

  // Send the hit columns to the hit operator
  auto hitMessage = std::make_shared<TupleMessage>(hitTupleSet->toTupleSetV1(), this->name());
  ctx()->send(hitMessage, hitOperator_->name());

  // Send the missed column names to the miss operator
  auto missMessage = std::make_shared<ScanMessage>(missedSegmentColumnNames, this->name());
  ctx()->send(missMessage, missOperator_->name());

  ctx()->notifyComplete();
}

void CacheLoad::setHitOperator(const std::shared_ptr<Operator> &op) {
  this->hitOperator_ = op;
  this->produce(op);
}

void CacheLoad::setMissOperator(const std::shared_ptr<Operator> &op) {
  this->missOperator_ = op;
  this->produce(op);
}
