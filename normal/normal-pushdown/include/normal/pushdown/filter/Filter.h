//
// Created by matt on 6/5/20.
//

#ifndef NORMAL_NORMAL_PUSHDOWN_INCLUDE_NORMAL_PUSHDOWN_FILTER_FILTER_H
#define NORMAL_NORMAL_PUSHDOWN_INCLUDE_NORMAL_PUSHDOWN_FILTER_FILTER_H

#include <memory>

#include <normal/core/Operator.h>
#include <normal/pushdown/TupleMessage.h>
#include <normal/core/message/CompleteMessage.h>
#include <normal/tuple/TupleSet2.h>
#include <normal/expression/Filter.h>

#include "FilterPredicate.h"

namespace normal::pushdown::filter {

class Filter : public normal::core::Operator {
public:
  explicit Filter(std::string Name, std::shared_ptr<FilterPredicate> Pred);

  static std::shared_ptr<Filter> make(const std::string &Name, const std::shared_ptr<FilterPredicate> &Pred);

  void onReceive(const core::message::Envelope &Envelope) override;

private:
  std::shared_ptr<FilterPredicate> pred_;

  std::optional<std::shared_ptr<normal::expression::Filter>> filter_;

  /**
   * A buffer of received tuples not yet filtered
   */
  std::shared_ptr<normal::tuple::TupleSet2> received_;

  /**
   * A buffer of filtered tuples not yet sent
   */
  std::shared_ptr<normal::tuple::TupleSet2> filtered_;

  void onStart();
  void onTuple(const normal::core::message::TupleMessage& Message);
  void onComplete(const normal::core::message::CompleteMessage& Message);

  void bufferTuples(const core::message::TupleMessage &Message);
  void buildFilter();
  void filterTuples();
  void sendTuples();
};

}

#endif //NORMAL_NORMAL_PUSHDOWN_INCLUDE_NORMAL_PUSHDOWN_FILTER_FILTER_H
