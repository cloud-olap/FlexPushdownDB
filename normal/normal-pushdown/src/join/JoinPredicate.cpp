//
// Created by matt on 29/4/20.
//

#include "normal/pushdown/join/JoinPredicate.h"

using namespace normal::pushdown::join;

JoinPredicate::JoinPredicate(const std::string &leftColumnName,
							 const std::string &rightColumnName) :
	leftColumnName_(leftColumnName),
	rightColumnName_(rightColumnName) {}
