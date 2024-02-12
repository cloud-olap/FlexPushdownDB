//
// Created by matt on 28/4/20.
//

#ifndef FPDB_FPDB_EXPRESSION_GANDIVA_INCLUDE_FPDB_EXPRESSION_GANDIVA_NUMERICLITERAL_H
#define FPDB_FPDB_EXPRESSION_GANDIVA_INCLUDE_FPDB_EXPRESSION_GANDIVA_NUMERICLITERAL_H

#include "Expression.h"
#include "DateIntervalType.h"
#include <fpdb/tuple/serialization/ArrowSerializer.h>
#include <arrow/api.h>
#include <gandiva/node.h>
#include <fmt/format.h>
#include <string>
#include <memory>
#include <sstream>

namespace fpdb::expression::gandiva {

template<typename ARROW_TYPE, typename C_TYPE = typename ARROW_TYPE::c_type>
class NumericLiteral : public Expression {

public:
  explicit NumericLiteral(std::optional<C_TYPE> value, std::optional<DateIntervalType> intervalType) :
    Expression(NUMERIC_LITERAL),
    value_(value),
    intervalType_(intervalType) {}
  NumericLiteral() = default;
  NumericLiteral(const NumericLiteral&) = default;
  NumericLiteral& operator=(const NumericLiteral&) = default;

  void compile(const std::shared_ptr<arrow::Schema> &) override;

  std::string alias() override {
    const std::shared_ptr<arrow::DataType> &arrowType = ::arrow::TypeTraits<ARROW_TYPE>::type_singleton();
    if (arrowType->id() == arrow::Type::INT32 || arrowType->id() == arrow::Type::INT64) {
      if (value_.has_value()) {
        return prefixInt_ + std::to_string(*value_);
      } else {
        return prefixInt_ + "0";
      }
    } else if (arrowType->id() == arrow::Type::DOUBLE) {
      if (value_.has_value()) {
        return prefixFloat_ + std::to_string(*value_);
      } else {
        return prefixFloat_ + "0.0";
      }
    } else {
      if (value_.has_value()) {
        return std::to_string(*value_);
      } else {
        return "0";
      }
    }
  }

  std::string getTypeString() const override;

  std::set<std::string> involvedColumnNames() override{
    return {};
  }

  ::nlohmann::json toJson() const override {
    ::nlohmann::json jObj;
    jObj.emplace("type", "NumericLiteral");
    jObj.emplace("dataType", fpdb::tuple::ArrowSerializer::dataType_to_bytes(
            ::arrow::TypeTraits<ARROW_TYPE>::type_singleton()));
    if (value_.has_value()) {
      jObj.emplace("value", *value_);
    }
    if (intervalType_.has_value()) {
      jObj.emplace("intervalType", intervalTypeToString(*intervalType_));
    }
    return jObj;
  }

  static tl::expected<std::shared_ptr<NumericLiteral>, std::string> fromJson(const nlohmann::json &jObj) {
    std::optional<C_TYPE> value = std::nullopt;
    if (jObj.contains("value")) {
      value = jObj["value"].get<C_TYPE>();
    }

    std::optional<DateIntervalType> intervalType = std::nullopt;
    if (jObj.contains("intervalType")) {
      auto expIntervalType = stringToIntervalType(jObj["intervalType"].get<std::string>());
      if (!expIntervalType.has_value()) {
        return tl::make_unexpected(expIntervalType.error());
      }
      intervalType = *expIntervalType;
    }

    return std::make_shared<NumericLiteral>(value, intervalType);
  }

  std::optional<C_TYPE> value() const {
    return value_;
  }

  const std::optional<DateIntervalType> &getIntervalType() const {
    return intervalType_;
  }

  // make the opposite literal of the current one, e.g. 5 -> -5
  void makeOpposite() {
    if (value_.has_value()) {
      value_ = std::optional<C_TYPE>(-*value_);
    } else {
      throw std::runtime_error("Cannot make opposite on null literal");
    }
  }

private:
  std::optional<C_TYPE> value_;
  std::optional<DateIntervalType> intervalType_;    // denote whether this literal is used as an interval

// caf inspect
public:
  template <class Inspector>
  friend bool inspect(Inspector& f, NumericLiteral& expr) {
    return f.object(expr).fields(f.field("type", expr.type_),
                                 f.field("value", expr.value_),
                                 f.field("intervalType", expr.intervalType_));
  }
};

template<typename ARROW_TYPE, typename C_TYPE = typename ARROW_TYPE::c_type>
std::shared_ptr<Expression> num_lit(std::optional<C_TYPE> value,
                                    std::optional<DateIntervalType> intervalType = std::nullopt){
  return std::make_shared<NumericLiteral<ARROW_TYPE>>(value, intervalType);
}

}

#endif //FPDB_FPDB_EXPRESSION_GANDIVA_INCLUDE_FPDB_EXPRESSION_GANDIVA_NUMERICLITERAL_H
