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
					 std::vector<std::string> columnNames,
					 std::shared_ptr<Partition> partition,
					 int64_t startOffset,
					 int64_t finishOffset);
  ~CacheLoad() override = default;

  static std::shared_ptr<CacheLoad> make(const std::string &name,
										 const std::vector<std::string>& columnNames,
										 const std::shared_ptr<Partition>& partition,
										 int64_t startOffset,
										 int64_t finishOffset);

  void onStart();
  void onReceive(const Envelope &msg) override;

  void setHitOperator(const std::shared_ptr<Operator> &op);
  void setMissOperator(const std::shared_ptr<Operator> &op);

private:
  std::vector<std::string> columnNames_;
  std::shared_ptr<Partition> partition_;
  int64_t startOffset_;
  int64_t finishOffset_;
//  std::optional<LocalOperatorDirectoryEntry> missOperatorEntry_;
//  std::optional<LocalOperatorDirectoryEntry> hitOperatorEntry_;

  std::shared_ptr<Operator> missOperator_;
  std::shared_ptr<Operator> hitOperator_;

  void requestLoadSegmentsFromCache();
  void onCacheLoadResponse(const LoadResponseMessage &Message);

};

}

#endif //NORMAL_NORMAL_PUSHDOWN_INCLUDE_NORMAL_PUSHDOWN_CACHE_CACHELOAD_H
