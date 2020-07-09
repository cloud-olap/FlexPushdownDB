//
// Created by matt on 8/7/20.
//

#include "normal/pushdown/cache/CacheLoad.h"

#include <normal/pushdown/cache/CacheHelper.h>
#include <normal/pushdown/TupleMessage.h>

using namespace normal::pushdown::cache;
using namespace normal::core::message;

CacheLoad::CacheLoad(std::string name,
					 const std::vector<std::string> &ColumnNames,
					 const std::shared_ptr<Partition> &Partition,
					 int64_t StartOffset,
					 int64_t FinishOffset) : Operator(std::move(name), "CacheLoad"),
											 columnNames_(ColumnNames), partition_(Partition),
											 startOffset_(StartOffset), finishOffset_(FinishOffset) {

}

std::shared_ptr<CacheLoad> CacheLoad::make(const std::string &name,
										   std::vector<std::string> &columnNames,
										   std::shared_ptr<Partition> &partition,
										   int64_t startOffset,
										   int64_t finishOffset) {
  return std::make_shared<CacheLoad>(name,
									 columnNames,
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
  requestLoadSegmentsFromCache();
}

void CacheLoad::requestLoadSegmentsFromCache() {
  CacheHelper::requestLoadSegmentsFromCache(columnNames_, partition_, startOffset_, finishOffset_, name(), ctx());
}

void CacheLoad::onCacheLoadResponse(const LoadResponseMessage &Message) {

  std::vector<std::string> missedSegmentColumnNames;
  std::vector<std::shared_ptr<Column>> hitSegmentColumns;

  auto hitSegments = Message.getSegments();

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

  // Send the missed column names to the miss operator
  auto missMessage = std::make_shared<TupleMessage>(hitTupleSet->toTupleSetV1(), this->name());
  ctx()->send(missMessage, missOperatorEntry_->name());

  // Send the hit columns to the hit operator
  auto hitMessage = std::make_shared<TupleMessage>(hitTupleSet->toTupleSetV1(), this->name());
  ctx()->send(hitMessage, hitOperatorEntry_->name());
}
