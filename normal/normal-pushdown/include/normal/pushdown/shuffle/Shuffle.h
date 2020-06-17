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
  Shuffle(const std::string &Name, std::string ColumnName);

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

private:
	std::string columnName_;

	std::vector<LocalOperatorDirectoryEntry> consumers_;
};

}

#endif //NORMAL_NORMAL_PUSHDOWN_INCLUDE_NORMAL_PUSHDOWN_SHUFFLE_SHUFFLE_H
