//
// Created by matt on 14/4/20.
//

#ifndef NORMAL_NORMAL_PUSHDOWN_SRC_PROJECT_H
#define NORMAL_NORMAL_PUSHDOWN_SRC_PROJECT_H

#include <normal/core/Operator.h>
#include <normal/core/expression/Expression.h>
#include <normal/core/message/TupleMessage.h>
#include <normal/core/message/CompleteMessage.h>

namespace normal::pushdown {

class Project : public normal::core::Operator {

public:

  /**
   * Constructor
   * @param Name Descriptive name
   * @param Expressions Expressions to evaluate to produce the attributes in the projection
   */
  Project(const std::string &Name,
          std::vector<std::shared_ptr<normal::core::expression::Expression>> Expressions);

  /**
   * Default destructor
   */
  ~Project() override = default;

  /**
   * Operators message handler
   * @param msg
   */
  void onReceive(const normal::core::message::Envelope &msg) override;

  /**
   * Start message handler
   */
  void onStart();

  /**
   * Completion message handler
   */
  void onComplete(const normal::core::message::CompleteMessage &);

  /**
   * Tuples message handler
   * @param message
   */
  void onTuple(const normal::core::message::TupleMessage &message);

private:
  /**
   * The expressions defining the attributes the projection should include
   */
  std::vector<std::shared_ptr<normal::core::expression::Expression>> expressions_;

  /**
   * A buffer of received tuples that are not projected until enough tuples have been received
   */
  std::shared_ptr<normal::core::TupleSet> tuples_;

  /**
   * Adds the tuples in the tuple message to the internal buffer
   * @param message
   */
  void bufferTuples(const core::message::TupleMessage &message);

  /**
   * Sends the given projected tuples to consumers
   * @param projected
   */
  void sendTuples(std::shared_ptr<normal::core::TupleSet> &projected);

  /**
   * Projects the tuples and sends them to consumers
   */
  void projectAndSendTuples();
};

}

#endif //NORMAL_NORMAL_PUSHDOWN_SRC_PROJECT_H
