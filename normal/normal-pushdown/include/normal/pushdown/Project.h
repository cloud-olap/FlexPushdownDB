//
// Created by matt on 14/4/20.
//

#ifndef NORMAL_NORMAL_PUSHDOWN_SRC_PROJECT_H
#define NORMAL_NORMAL_PUSHDOWN_SRC_PROJECT_H

#include <normal/core/Operator.h>
#include <normal/expression/Expression.h>
#include <normal/expression/Projector.h>
#include <normal/expression/gandiva/Expression.h>
#include <normal/pushdown/TupleMessage.h>
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
          std::vector<std::shared_ptr<normal::expression::gandiva::Expression>> Expressions,
          long queryId = 0);

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
  std::vector<std::shared_ptr<normal::expression::gandiva::Expression>> expressions_;

  /**
   * The schema of received tuples, sometimes cannot be known up front (e.g. when input source is a CSV file, the
   * columns aren't known until the file is read) so needs to be extracted from the first batch of tuples received
   */
  std::optional<std::shared_ptr<arrow::Schema>> inputSchema_;

  /**
   * A buffer of received tuples that are not projected until enough tuples have been received
   */
  std::shared_ptr<TupleSet> tuples_;

  /**
   * The expression projector, created and cached when input schema is extracted from first tuple received
   */
  std::optional<std::shared_ptr<normal::expression::Projector>> projector_;

  /**
   * Adds the tuples in the tuple message to the internal buffer
   * @param message
   */
  void bufferTuples(const core::message::TupleMessage &message);

  /**
   * Sends the given projected tuples to consumers
   * @param projected
   */
  void sendTuples(std::shared_ptr<TupleSet> &projected);

  /**
   * Projects the tuples and sends them to consumers
   */
  void projectAndSendTuples();
  void cacheInputSchema(const core::message::TupleMessage &message);
  void buildAndCacheProjector();

};

}

#endif //NORMAL_NORMAL_PUSHDOWN_SRC_PROJECT_H
