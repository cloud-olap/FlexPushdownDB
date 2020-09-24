//
// Created by matt on 17/6/20.
//

#ifndef NORMAL_NORMAL_PUSHDOWN_INCLUDE_NORMAL_PUSHDOWN_SHUFFLE_SHUFFLE_H
#define NORMAL_NORMAL_PUSHDOWN_INCLUDE_NORMAL_PUSHDOWN_SHUFFLE_SHUFFLE_H

#include <normal/core/Operator.h>
#include <normal/core/message/CompleteMessage.h>
#include <normal/pushdown/TupleMessage.h>

using namespace normal::core;
using namespace normal::core::message;

namespace normal::pushdown::shuffle {

/**
 * A firts cut of a shuffle operator, shuffles on a single key only
 */
class Shuffle : public Operator {

public:
  Shuffle(const std::string &Name, std::string ColumnName, long queryId);

  static std::shared_ptr<Shuffle> make(const std::string &Name, const std::string& ColumnName, long queryId = 0);

  /**
   * Operators message handler
   * @param msg
   */
  void onReceive(const Envelope &msg) override;

  /**
   * Start message handler
   */
  void onStart();

  /**
   * Completion message handler
   */
  void onComplete(const CompleteMessage &);

  /**
   * Tuples message handler
   * @param message
   */
  void onTuple(const TupleMessage &message);
  void produce(const std::shared_ptr<Operator> &operator_) override;

private:
	std::string columnName_;

	std::vector<std::string> consumers_;
};

}

#endif //NORMAL_NORMAL_PUSHDOWN_INCLUDE_NORMAL_PUSHDOWN_SHUFFLE_SHUFFLE_H
