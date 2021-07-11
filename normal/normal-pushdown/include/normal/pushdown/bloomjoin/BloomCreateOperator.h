//
// Created by matt on 5/8/20.
//

#ifndef NORMAL_NORMAL_PUSHDOWN_INCLUDE_NORMAL_PUSHDOWN_BLOOMJOIN_BLOOMCREATEOPERATOR_H
#define NORMAL_NORMAL_PUSHDOWN_INCLUDE_NORMAL_PUSHDOWN_BLOOMJOIN_BLOOMCREATEOPERATOR_H

#include <utility>

#include <normal/core/Operator.h>
#include <normal/core/message/CompleteMessage.h>
#include <normal/core/message/TupleMessage.h>

#include "BloomCreateKernel.h"
#include "BloomFilterMessage.h"

using namespace normal::core;
using namespace normal::core::message;

namespace normal::pushdown::bloomjoin {

class BloomCreateOperator : public Operator {
public:
  explicit BloomCreateOperator(const std::string &name,
							   const std::string &columnName,
							   double desiredFalsePositiveRate,
							   const std::vector<std::string> &bloomJoinUseSqlTemplates,
							   long queryId) :
	  Operator(name, "BloomCreateOperator", queryId),
	  kernel_(BloomCreateKernel::make(columnName, desiredFalsePositiveRate, bloomJoinUseSqlTemplates)) {
  }

  static std::shared_ptr<BloomCreateOperator> make(const std::string &name,
                                                   const std::string &columnName,
                                                   double desiredFalsePositiveRate,
                                                   const std::vector<std::string> &bloomJoinUseSqlTemplates,
                                                   long queryId = 0) {
	return std::make_shared<BloomCreateOperator>(name,
	                                             columnName,
	                                             desiredFalsePositiveRate,
	                                             bloomJoinUseSqlTemplates,
	                                             queryId);
  }

  void onReceive(const Envelope &msg) override {
	if (msg.message().type() == "StartMessage") {
	  this->onStart();
	} else if (msg.message().type() == "TupleMessage") {
	  auto tupleMessage = dynamic_cast<const TupleMessage &>(msg.message());
	  this->onTuple(tupleMessage);
	} else if (msg.message().type() == "CompleteMessage") {
	  auto completeMessage = dynamic_cast<const CompleteMessage &>(msg.message());
	  this->onComplete(completeMessage);
	} else {
	  // FIXME: Propagate error properly
	  throw std::runtime_error("Unrecognized message type " + msg.message().type());
	}
  }

private:

  std::shared_ptr<BloomCreateKernel> kernel_;

  void onStart() {
	SPDLOG_DEBUG("Starting operator  |  name: '{}'", this->name());
  }

  void onTuple(const TupleMessage &msg) {
	auto result = kernel_->addTupleSet(TupleSet2::create(msg.tuples()));
	if (!result)
	  throw std::runtime_error(result.error());
  }

  void onComplete(const CompleteMessage &/*msg*/) {
	if (weakCtx()->operatorMap().allComplete(OperatorRelationshipType::Producer)) {

	  auto result = kernel_->buildBloomFilter();
	  if (!result)
		throw std::runtime_error(result.error());

	  auto expectedBloomFilter = kernel_->getBloomFilter();
	  if (!expectedBloomFilter)
		throw std::runtime_error("No bloom filter");
	  auto bloomFilter = expectedBloomFilter.value();

	  std::shared_ptr<Message>
		  bloomFilterMessage = std::make_shared<BloomFilterMessage>(bloomFilter, name());
	  weakCtx()->tell(bloomFilterMessage);

	  weakCtx()->notifyComplete();
	}
  }

};

}

#endif //NORMAL_NORMAL_PUSHDOWN_INCLUDE_NORMAL_PUSHDOWN_BLOOMJOIN_BLOOMCREATEOPERATOR_H
