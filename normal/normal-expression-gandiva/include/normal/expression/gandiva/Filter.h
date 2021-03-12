//
// Created by matt on 6/5/20.
//

#ifndef NORMAL_NORMAL_EXPRESSION_GANDIVA_INCLUDE_NORMAL_EXPRESSION_GANDIVA_FILTER_H
#define NORMAL_NORMAL_EXPRESSION_GANDIVA_INCLUDE_NORMAL_EXPRESSION_GANDIVA_FILTER_H

#include <normal/expression/gandiva/Expression.h>
#include <gandiva/expression.h>
#include <gandiva/filter.h>
#include <normal/expression/Filter.h>
#include "Projector.h"
#include <map>

namespace normal::expression::gandiva {

class Filter : public normal::expression::Filter {

public:
  explicit Filter(std::shared_ptr<Expression> Pred);

  static std::shared_ptr<Filter> make(const std::shared_ptr<Expression> &Pred);

  std::shared_ptr<normal::tuple::TupleSet2> evaluate(const normal::tuple::TupleSet2 &TupleSet) override;
  void compile(const std::shared_ptr<normal::tuple::Schema> &schema) override;

private:
  std::shared_ptr<Expression> pred_;
  std::shared_ptr<::gandiva::Filter> gandivaFilter_;
  std::shared_ptr<::gandiva::Projector> gandivaProjector_;
};

/**
 * A global map to store compiled ::gandiva::Project and ::gandiva::Filter
 */
inline std::map<std::string, std::shared_ptr<::gandiva::Filter>> gandivaFilterMap_;
inline std::map<std::string, std::shared_ptr<::gandiva::Projector>> gandivaProjectorMap_;

/**
 * Build and store ::gandiva::Filter
 */
std::shared_ptr<::gandiva::Filter> buildGandivaFilter(const std::shared_ptr<::gandiva::Condition> &gandivaCondition,
                                                      const std::shared_ptr<normal::tuple::Schema> &schema);

/**
 * Build and store ::gandiva::Projector
 */
std::shared_ptr<::gandiva::Projector> buildGandivaProjector(const std::shared_ptr<normal::tuple::Schema> &schema);

/**
 * Clear ::gandiva::Projector and ::gandiva::Filter maps
 */
void clearGandivaProjectorAndFilter();

}

#endif //NORMAL_NORMAL_EXPRESSION_GANDIVA_INCLUDE_NORMAL_EXPRESSION_GANDIVA_FILTER_H
