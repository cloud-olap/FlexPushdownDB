//
// Created by matt on 8/7/20.
//

#ifndef NORMAL_NORMAL_PUSHDOWN_INCLUDE_NORMAL_PUSHDOWN_CACHE_CACHELOAD_H
#define NORMAL_NORMAL_PUSHDOWN_INCLUDE_NORMAL_PUSHDOWN_CACHE_CACHELOAD_H

#include <normal/core/Operator.h>
#include <normal/core/message/Envelope.h>
#include <normal/connector/partition/Partition.h>
#include <normal/core/cache/LoadResponseMessage.h>

using namespace normal::core;
using namespace normal::core::message;
using namespace normal::core::cache;

namespace normal::pushdown::cache {

class CacheLoad : public Operator {

public:
  explicit CacheLoad(std::string name,
					 std::vector<std::string> projectedColumnNames,
					 std::vector<std::string> predicateColumnNames,
					 std::shared_ptr<Partition> partition,
					 int64_t startOffset,
					 int64_t finishOffset,
					 bool useNewCacheLayout,
					 long queryId);
  ~CacheLoad() override = default;

  static std::shared_ptr<CacheLoad> make(const std::string &name,
                     std::vector<std::string> projectedColumnNames,
                     std::vector<std::string> predicateColumnNames,
										 const std::shared_ptr<Partition>& partition,
										 int64_t startOffset,
										 int64_t finishOffset,
										 bool useNewCacheLayout,
										 long queryId = 0);

  void onStart();
  void onReceive(const Envelope &msg) override;

  void setHitOperator(const std::shared_ptr<Operator> &op);
  void setMissOperatorToCache(const std::shared_ptr<Operator> &op);
  void setMissOperatorToPushdown(const std::shared_ptr<Operator> &op);

private:
  /**
   * columnNames = projectedColumnNames + predicateColumnNames
   */
  std::vector<std::string> columnNames_;
  std::vector<std::string> projectedColumnNames_;
  std::vector<std::string> predicateColumnNames_;

  std::shared_ptr<Partition> partition_;
  int64_t startOffset_;
  int64_t finishOffset_;

  std::weak_ptr<Operator> hitOperator_;
  std::weak_ptr<Operator> missOperatorToCache_;
  std::weak_ptr<Operator> missOperatorToPushdown_;

  /**
   * whether to use the new cache layout after segments back or the last one without waiting
   */
  bool useNewCacheLayout_;

  void requestLoadSegmentsFromCache();
  void onCacheLoadResponse(const LoadResponseMessage &Message);

};

}

#endif //NORMAL_NORMAL_PUSHDOWN_INCLUDE_NORMAL_PUSHDOWN_CACHE_CACHELOAD_H
