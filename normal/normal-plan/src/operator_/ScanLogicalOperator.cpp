//
// Created by matt on 1/4/20.
//

#include <normal/plan/operator_/ScanLogicalOperator.h>
#include <normal/plan/operator_/type/OperatorTypes.h>
#include <normal/expression/gandiva/And.h>
#include <normal/expression/gandiva/LessThan.h>
#include <normal/expression/gandiva/Column.h>
#include <normal/expression/gandiva/StringLiteral.h>
#include <normal/expression/gandiva/NumericLiteral.h>
#include <normal/connector/MiniCatalogue.h>
#include <normal/expression/gandiva/LessThanOrEqualTo.h>
#include <normal/expression/gandiva/GreaterThan.h>
#include <normal/expression/gandiva/GreaterThanOrEqualTo.h>
#include <normal/expression/gandiva/EqualTo.h>
#include <normal/expression/gandiva/Or.h>

using namespace normal::plan::operator_;

ScanLogicalOperator::ScanLogicalOperator(
	std::shared_ptr<PartitioningScheme> partitioningScheme) :
	LogicalOperator(type::OperatorTypes::scanOperatorType()),
	partitioningScheme_(std::move(partitioningScheme)) {}

const std::shared_ptr<PartitioningScheme> &ScanLogicalOperator::getPartitioningScheme() const {
  return partitioningScheme_;
}

void ScanLogicalOperator::predicate(const std::shared_ptr<expression::gandiva::Expression> &predicate) {
  predicate_ = predicate;
}

void ScanLogicalOperator::setPredicate(const std::shared_ptr<expression::gandiva::Expression> &predicate) {
  predicate_ = predicate;
}

void
ScanLogicalOperator::setProjectedColumnNames(const std::shared_ptr<std::vector<std::string>> &projectedColumnNames) {
  projectedColumnNames_ = projectedColumnNames;
}

const std::shared_ptr<std::vector<std::shared_ptr<normal::core::Operator>>> &
ScanLogicalOperator::streamOutPhysicalOperators() const {
  return streamOutPhysicalOperators_;
}

// FIXME: if numeric, compare using double in default
std::optional<double> getNumericValue(std::shared_ptr<normal::expression::gandiva::Expression> expr) {
  if (typeid(*expr) == typeid(normal::expression::gandiva::NumericLiteral<::arrow::Int32Type>)) {
    auto typedExpr = std::static_pointer_cast<normal::expression::gandiva::NumericLiteral<::arrow::Int32Type>>(expr);
    return (double) typedExpr->value();
  } else if (typeid(*expr) == typeid(normal::expression::gandiva::NumericLiteral<::arrow::Int64Type>)) {
    auto typedExpr = std::static_pointer_cast<normal::expression::gandiva::NumericLiteral<::arrow::Int64Type>>(expr);
    return (double) typedExpr->value();
  } else if (typeid(*expr) == typeid(normal::expression::gandiva::NumericLiteral<::arrow::FloatType>)) {
    auto typedExpr = std::static_pointer_cast<normal::expression::gandiva::NumericLiteral<::arrow::FloatType>>(expr);
    return (double) typedExpr->value();
  } else if (typeid(*expr) == typeid(normal::expression::gandiva::NumericLiteral<::arrow::DoubleType>)) {
    auto typedExpr = std::static_pointer_cast<normal::expression::gandiva::NumericLiteral<::arrow::DoubleType>>(expr);
    return (double) typedExpr->value();
  } else {
    return std::nullopt;
  }
}

std::shared_ptr<std::vector<std::shared_ptr<Partition>>> ScanLogicalOperator::getValidPartitions(
        std::shared_ptr<expression::gandiva::Expression> predicate) {
  auto sortedColumns = normal::connector::defaultMiniCatalogue->sortedColumns();

  if (typeid(*predicate) == typeid(expression::gandiva::And)) {
    auto andExpr = std::static_pointer_cast<expression::gandiva::And>(predicate);
    auto leftValidPartitions = getValidPartitions(andExpr->getLeft());
    auto rightValidPartitions = getValidPartitions(andExpr->getRight());
    // intersection
    auto leftValidPartitionSet = std::make_shared<std::unordered_set<std::shared_ptr<Partition>, PartitionPointerHash, PartitionPointerPredicate>>();
    for (auto const &leftValidPartition: *leftValidPartitions) {
      leftValidPartitionSet->emplace(leftValidPartition);
    }
    auto intersectionSet = std::make_shared<std::unordered_set<std::shared_ptr<Partition>, PartitionPointerHash, PartitionPointerPredicate>>();
    for (auto const &rightValidPartition: *rightValidPartitions) {
      if (leftValidPartitionSet->find(rightValidPartition) != leftValidPartitionSet->end()) {
        intersectionSet->emplace(rightValidPartition);
      }
    }
    auto validPartitions = std::make_shared<std::vector<std::shared_ptr<Partition>>>();
    validPartitions->assign(intersectionSet->begin(), intersectionSet->end());
    return validPartitions;
  }

  else if (typeid(*predicate) == typeid(expression::gandiva::Or)) {
    auto andExpr = std::static_pointer_cast<expression::gandiva::Or>(predicate);
    auto leftValidPartitions = getValidPartitions(andExpr->getLeft());
    auto rightValidPartitions = getValidPartitions(andExpr->getRight());
    // union
    auto unionSet = std::make_shared<std::unordered_set<std::shared_ptr<Partition>, PartitionPointerHash, PartitionPointerPredicate>>();
    for (auto const &validPartition: *leftValidPartitions) {
      unionSet ->emplace(validPartition);
    }
    auto validPartitions = std::make_shared<std::vector<std::shared_ptr<Partition>>>();
    validPartitions->assign(unionSet->begin(), unionSet->end());
    return validPartitions;
  }

  else if (typeid(*predicate) == typeid(expression::gandiva::LessThan) ||
           typeid(*predicate) == typeid(expression::gandiva::LessThanOrEqualTo) ||
           typeid(*predicate) == typeid(expression::gandiva::GreaterThan) ||
           typeid(*predicate) == typeid(expression::gandiva::GreaterThanOrEqualTo) ||
           typeid(*predicate) == typeid(expression::gandiva::EqualTo)) {
    auto ltExpr = std::static_pointer_cast<expression::gandiva::BinaryExpression>(predicate);
    auto leftExpr = ltExpr->getLeft();
    auto rightExpr = ltExpr->getRight();

    // try to get valid partitions
    if (typeid(*leftExpr) == typeid(expression::gandiva::Column)) {
      auto columnName = std::static_pointer_cast<expression::gandiva::Column>(leftExpr)->getColumnName();
      if (typeid(*rightExpr) == typeid(expression::gandiva::StringLiteral)) {
        auto sortedColumnValuesIt = sortedColumns->find(columnName);
        if (sortedColumnValuesIt != sortedColumns->end()) {
          auto sortedColumnValues = sortedColumnValuesIt->second;
          auto predicateValue = std::static_pointer_cast<expression::gandiva::StringLiteral>(rightExpr)->value();
          auto validPartitions = std::make_shared<std::vector<std::shared_ptr<Partition>>>();
          for (auto const &partition: *getPartitioningScheme()->partitions()) {
            auto dataValuePair = sortedColumnValues->find(partition)->second;
            if (typeid(*predicate) == typeid(expression::gandiva::LessThan)) {
              if (dataValuePair.first.compare(predicateValue) < 0) {
                validPartitions->emplace_back(partition);
              }
            } else if (typeid(*predicate) == typeid(expression::gandiva::LessThanOrEqualTo)) {
              if (dataValuePair.first.compare(predicateValue) <= 0) {
                validPartitions->emplace_back(partition);
              }
            } else if (typeid(*predicate) == typeid(expression::gandiva::GreaterThan)) {
              if (dataValuePair.second.compare(predicateValue) > 0) {
                validPartitions->emplace_back(partition);
              }
            } else if (typeid(*predicate) == typeid(expression::gandiva::GreaterThanOrEqualTo)) {
              if (dataValuePair.second.compare(predicateValue) >= 0) {
                validPartitions->emplace_back(partition);
              }
            } else {
              if (dataValuePair.first.compare(predicateValue) <= 0 && dataValuePair.second.compare(predicateValue) >= 0) {
                validPartitions->emplace_back(partition);
              }
            }
          }
          return validPartitions;
        }
      } else {
        auto numericValue = getNumericValue(rightExpr);
        if (numericValue.has_value()) {
          auto predicateValue = numericValue.value();
          auto sortedColumnValuesIt = sortedColumns->find(columnName);
          if (sortedColumnValuesIt != sortedColumns->end()) {
            auto sortedColumnValues = sortedColumnValuesIt->second;
            auto validPartitions = std::make_shared<std::vector<std::shared_ptr<Partition>>>();
            for (auto const &partition: *getPartitioningScheme()->partitions()) {
              auto dataValuePair = sortedColumnValues->find(partition)->second;
              if (typeid(*predicate) == typeid(expression::gandiva::LessThan)) {
                if (std::stod(dataValuePair.first) < predicateValue) {
                  validPartitions->emplace_back(partition);
                }
              } else if (typeid(*predicate) == typeid(expression::gandiva::LessThanOrEqualTo)) {
                if (std::stod(dataValuePair.first) <= predicateValue) {
                  validPartitions->emplace_back(partition);
                }
              } else if (typeid(*predicate) == typeid(expression::gandiva::GreaterThan)) {
                if (std::stod(dataValuePair.second) > predicateValue) {
                  validPartitions->emplace_back(partition);
                }
              } else if (typeid(*predicate) == typeid(expression::gandiva::GreaterThanOrEqualTo)) {
                if (std::stod(dataValuePair.second) >= predicateValue) {
                  validPartitions->emplace_back(partition);
                }
              } else {
                if (std::stod(dataValuePair.first) <= predicateValue && std::stod(dataValuePair.second) >= predicateValue) {
                  validPartitions->emplace_back(partition);
                }
              }
            }
            return validPartitions;
          }
        }
      }
    }

    else if (typeid(*rightExpr) == typeid(expression::gandiva::Column)) {
      auto columnName = std::static_pointer_cast<expression::gandiva::Column>(rightExpr)->getColumnName();
      if (typeid(*leftExpr) == typeid(expression::gandiva::StringLiteral)) {
        auto sortedColumnValuesIt = sortedColumns->find(columnName);
        if (sortedColumnValuesIt != sortedColumns->end()) {
          auto sortedColumnValues = sortedColumnValuesIt->second;
          auto predicateValue = std::static_pointer_cast<expression::gandiva::StringLiteral>(leftExpr)->value();
          auto validPartitions = std::make_shared<std::vector<std::shared_ptr<Partition>>>();
          for (auto const &partition: *getPartitioningScheme()->partitions()) {
            auto dataValuePair = sortedColumnValues->find(partition)->second;
            if (typeid(*predicate) == typeid(expression::gandiva::LessThan)) {
              if (dataValuePair.second.compare(predicateValue) > 0) {
                validPartitions->emplace_back(partition);
              }
            } else if (typeid(*predicate) == typeid(expression::gandiva::LessThanOrEqualTo)) {
              if (dataValuePair.second.compare(predicateValue) >= 0) {
                validPartitions->emplace_back(partition);
              }
            } else if (typeid(*predicate) == typeid(expression::gandiva::GreaterThan)) {
              if (dataValuePair.first.compare(predicateValue) < 0) {
                validPartitions->emplace_back(partition);
              }
            } else if (typeid(*predicate) == typeid(expression::gandiva::GreaterThanOrEqualTo)) {
              if (dataValuePair.first.compare(predicateValue) <= 0) {
                validPartitions->emplace_back(partition);
              }
            } else {
              if (dataValuePair.first.compare(predicateValue) <= 0 && dataValuePair.second.compare(predicateValue) >= 0) {
                validPartitions->emplace_back(partition);
              }
            }
          }
          return validPartitions;
        }
      } else {
        auto numericValue = getNumericValue(leftExpr);
        if (numericValue.has_value()) {
          auto predicateValue = numericValue.value();
          auto sortedColumnValuesIt = sortedColumns->find(columnName);
          if (sortedColumnValuesIt != sortedColumns->end()) {
            auto sortedColumnValues = sortedColumnValuesIt->second;
            auto validPartitions = std::make_shared<std::vector<std::shared_ptr<Partition>>>();
            for (auto const &partition: *getPartitioningScheme()->partitions()) {
              auto dataValuePair = sortedColumnValues->find(partition)->second;
              if (typeid(*predicate) == typeid(expression::gandiva::LessThan)) {
                if (std::stod(dataValuePair.second) > predicateValue) {
                  validPartitions->emplace_back(partition);
                }
              } else if (typeid(*predicate) == typeid(expression::gandiva::LessThanOrEqualTo)) {
                if (std::stod(dataValuePair.second) >= predicateValue) {
                  validPartitions->emplace_back(partition);
                }
              } else if (typeid(*predicate) == typeid(expression::gandiva::GreaterThan)) {
                if (std::stod(dataValuePair.first) < predicateValue) {
                  validPartitions->emplace_back(partition);
                }
              } else if (typeid(*predicate) == typeid(expression::gandiva::GreaterThanOrEqualTo)) {
                if (std::stod(dataValuePair.first) <= predicateValue) {
                  validPartitions->emplace_back(partition);
                }
              } else {
                if (std::stod(dataValuePair.first) <= predicateValue && std::stod(dataValuePair.second) >= predicateValue) {
                  validPartitions->emplace_back(partition);
                }
              }
            }
            return validPartitions;
          }
        }
      }
    }

    // all partitions are valid
    auto validPartitions = std::make_shared<std::vector<std::shared_ptr<Partition>>>();
    auto allPartitions = getPartitioningScheme()->partitions();
    validPartitions->assign(allPartitions->begin(), allPartitions->end());
    return validPartitions;
  }

  else {
    throw std::runtime_error("Bad filter predicate");
  }
}
