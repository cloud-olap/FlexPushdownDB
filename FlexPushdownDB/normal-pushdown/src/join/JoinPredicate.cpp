//
// Created by matt on 29/4/20.
//

#include <algorithm>
#include <utility>
#include "normal/pushdown/join/JoinPredicate.h"

using namespace normal::pushdown::join;

JoinPredicate::JoinPredicate(std::string leftColumnName,
							 std::string rightColumnName) :
	leftColumnName_(std::move(leftColumnName)),
	rightColumnName_(std::move(rightColumnName)) {}

JoinPredicate JoinPredicate::create(const std::string &leftColumnName, const std::string &rightColumnName) {

  std::string l(leftColumnName);
  std::string r(rightColumnName);

  std::transform(l.begin(), l.end(), l.begin(), ::tolower);
  std::transform(r.begin(), r.end(), r.begin(), ::tolower);
  return JoinPredicate(l, r);
}

const std::string &JoinPredicate::getLeftColumnName() const {
  return leftColumnName_;
}

const std::string &JoinPredicate::getRightColumnName() const {
  return rightColumnName_;
}
