//
// Created by matt on 2/6/20.
//

#include <normal/connector/MiniCatalogue.h>
#include "normal/pushdown/cache/CacheHelper.h"

using namespace normal::pushdown::cache;

void CacheHelper::requestLoadSegmentsFromCache(const std::vector<std::string> &columnNames,
											   const std::shared_ptr<Partition> &partition,
											   int64_t startOffset,
											   int64_t finishOffset,
											   const std::string &sender,
											   const std::shared_ptr<OperatorContext> &ctx) {

  assert(partition);
  assert(startOffset >= 0);
  assert(finishOffset > startOffset);
  assert(sender.size() > 0);
  assert(ctx);

  std::vector<std::shared_ptr<SegmentKey>> segmentKeys;
  auto miniCatalogue = normal::connector::defaultMiniCatalogue;
  for(const auto &columnName: columnNames){
    size_t estimateSize = (double) (finishOffset - startOffset) * miniCatalogue->lengthFraction(columnName);
    auto segmentKey = SegmentKey::make(partition,
                                       columnName,
                                       SegmentRange::make(startOffset, finishOffset),
                                       SegmentMetadata::make(estimateSize, 0));
    segmentKeys.emplace_back(segmentKey);
  }

  ctx->send(LoadRequestMessage::make(segmentKeys, sender), "SegmentCache")
	  .map_error([](auto err) { throw std::runtime_error(err); });
}

void CacheHelper::requestStoreSegmentsInCache(const std::shared_ptr<TupleSet2> &tupleSet,
											  const std::shared_ptr<Partition> &partition,
											  int64_t startOffset,
											  int64_t finishOffset,
											  const std::string &sender,
											  const std::shared_ptr<OperatorContext> &ctx) {

  assert(tupleSet);
  assert(startOffset >= 0);
  assert(finishOffset > startOffset);
  assert(sender.size() > 0);
  assert(ctx);

  std::unordered_map<std::shared_ptr<SegmentKey>, std::shared_ptr<SegmentData>> segmentsToStore;
  for (int64_t c = 0; c < tupleSet->numColumns(); ++c) {
	auto column = tupleSet->getColumnByIndex(c).value();
	auto segmentKey = SegmentKey::make(partition,
	                                   column->getName(),
	                                   SegmentRange::make(startOffset, finishOffset),
	                                   SegmentMetadata::make(0, column->size()));
	auto segmentData = SegmentData::make(column);

	segmentsToStore.emplace(segmentKey, segmentData);
  }

  ctx->send(StoreRequestMessage::make(segmentsToStore, sender), "SegmentCache")
	  .map_error([](auto err) { throw std::runtime_error(err); });
}
