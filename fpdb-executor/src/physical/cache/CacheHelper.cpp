//
// Created by matt on 2/6/20.
//

#include <fpdb/executor/physical/cache/CacheHelper.h>

using namespace fpdb::executor::physical::cache;
using namespace fpdb::executor::message;

void CacheHelper::requestLoadSegmentsFromCache(const std::vector<std::string> &columnNames,
											   const std::shared_ptr<Partition> &partition,
											   int64_t startOffset,
											   int64_t finishOffset,
											   const std::string &sender,
											   const std::shared_ptr<POpContext> &ctx) {

  assert(partition);
  assert(startOffset >= 0);
  assert(finishOffset > startOffset);
  assert(!sender.empty());
  assert(ctx);

  std::vector<std::shared_ptr<SegmentKey>> segmentKeys;
  for(const auto &columnName: columnNames){
    auto segmentKey = SegmentKey::make(partition,
                                       columnName,
                                       SegmentRange::make(startOffset, finishOffset),
                                       SegmentMetadata::make(0));
    segmentKeys.emplace_back(segmentKey);
  }

  ctx->send(LoadRequestMessage::make(segmentKeys, sender), "SegmentCache");
}

void CacheHelper::requestStoreSegmentsInCache(const std::shared_ptr<TupleSet> &tupleSet,
											  const std::shared_ptr<Partition> &partition,
											  int64_t startOffset,
											  int64_t finishOffset,
											  const std::string &sender,
											  const std::shared_ptr<POpContext> &ctx) {

  assert(tupleSet);
  assert(startOffset >= 0);
  assert(finishOffset > startOffset);
  assert(!sender.empty());
  assert(ctx);

  std::unordered_map<std::shared_ptr<SegmentKey>, std::shared_ptr<SegmentData>> segmentsToStore;
  for (int64_t c = 0; c < tupleSet->numColumns(); ++c) {
	auto column = tupleSet->getColumnByIndex(c).value();
	auto segmentKey = SegmentKey::make(partition,
	                                   column->getName(),
	                                   SegmentRange::make(startOffset, finishOffset),
	                                   SegmentMetadata::make(column->size()));
	auto segmentData = SegmentData::make(column);

	segmentsToStore.emplace(segmentKey, segmentData);
  }

  ctx->send(StoreRequestMessage::make(segmentsToStore, sender), "SegmentCache");
}
