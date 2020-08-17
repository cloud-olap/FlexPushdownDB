//
// Created by matt on 2/6/20.
//

#ifndef NORMAL_NORMAL_PUSHDOWN_INCLUDE_NORMAL_PUSHDOWN_CACHE_CACHEHELPER_H
#define NORMAL_NORMAL_PUSHDOWN_INCLUDE_NORMAL_PUSHDOWN_CACHE_CACHEHELPER_H

#include <memory>

#include <normal/tuple/TupleSet2.h>
#include <normal/cache/SegmentKey.h>
#include <normal/cache/SegmentData.h>
#include <normal/core/cache/StoreRequestMessage.h>
#include <normal/core/OperatorContext.h>
#include <normal/core/cache/LoadRequestMessage.h>

using namespace normal::tuple;
using namespace normal::cache;
using namespace normal::core;
using namespace normal::core::cache;

namespace normal::pushdown::cache {

class CacheHelper {

public:

  /**
   * Issues a request to load multiple columns from the cache for a single given partition and range
   *
   * @param partition
   * @param columnNames
   * @param startOffset
   * @param finishOffset
   * @param sender
   * @param ctx
   */
  static void requestLoadSegmentsFromCache(const std::vector<std::string> &columnNames,
										   const std::shared_ptr<Partition> &partition,
										   int64_t startOffset,
										   int64_t finishOffset,
										   const std::string &sender,
										   const std::shared_ptr<OperatorContext> &ctx);

  /**
   * Issues a request to store multiple columns in the cache for a single given partition and range
   *
   * @param partition
   * @param startOffset
   * @param finishOffset
   * @param tupleSet
   * @param sender
   * @param ctx
   * @param used
   */
  static void requestStoreSegmentsInCache(const std::shared_ptr<TupleSet2> &tupleSet,
										  const std::shared_ptr<Partition> &partition,
										  int64_t startOffset,
										  int64_t finishOffset,
										  const std::string &sender,
										  const std::shared_ptr<OperatorContext> &ctx,
										  bool used);
};

}

#endif //NORMAL_NORMAL_PUSHDOWN_INCLUDE_NORMAL_PUSHDOWN_CACHE_CACHEHELPER_H
