//
// Created by matt on 2/6/20.
//

#ifndef FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_CACHE_CACHEHELPER_H
#define FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_CACHE_CACHEHELPER_H

#include <fpdb/executor/physical/POpContext.h>
#include <fpdb/executor/message/cache/StoreRequestMessage.h>
#include <fpdb/executor/message/cache/LoadRequestMessage.h>
#include <fpdb/cache/SegmentKey.h>
#include <fpdb/cache/SegmentData.h>
#include <fpdb/tuple/TupleSet.h>
#include <memory>

using namespace fpdb::executor::physical;
using namespace fpdb::tuple;
using namespace fpdb::cache;

namespace fpdb::executor::physical::cache {

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
										   const std::shared_ptr<POpContext> &ctx);

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
  static void requestStoreSegmentsInCache(const std::shared_ptr<TupleSet> &tupleSet,
										  const std::shared_ptr<Partition> &partition,
										  int64_t startOffset,
										  int64_t finishOffset,
										  const std::string &sender,
										  const std::shared_ptr<POpContext> &ctx);
};

}

#endif //FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_CACHE_CACHEHELPER_H
