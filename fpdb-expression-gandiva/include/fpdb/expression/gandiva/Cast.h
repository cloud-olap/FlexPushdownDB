//
// Created by matt on 27/4/20.
//

#ifndef FPDB_FPDB_EXPRESSION_GANDIVA_INCLUDE_FPDB_EXPRESSION_GANDIVA_CAST_H
#define FPDB_FPDB_EXPRESSION_GANDIVA_INCLUDE_FPDB_EXPRESSION_GANDIVA_CAST_H

#include <fpdb/expression/gandiva/Expression.h>
#include <fpdb/tuple/serialization/ArrowSerializer.h>
#include <fpdb/tuple/TupleSet.h>
#include <arrow/api.h>
#include <gandiva/node.h>
#include <string>
#include <memory>

namespace fpdb::expression::gandiva {

class Cast : public Expression {

public:
  // Convert date32 to date64 for parquet
  static tl::expected<std::shared_ptr<fpdb::tuple::TupleSet>, std::string>
  castDate32ToDate64(const std::shared_ptr<fpdb::tuple::TupleSet> &tupleSet);

  Cast(std::shared_ptr<Expression> expr, std::shared_ptr<arrow::DataType> dataType);
  Cast() = default;
  Cast(const Cast&) = default;
  Cast& operator=(const Cast&) = default;

  void compile(const std::shared_ptr<arrow::Schema> &schema) override;
  std::string alias() override;
  std::string getTypeString() const override;
  std::set<std::string> involvedColumnNames() override;
  ::nlohmann::json toJson() const override;
  static tl::expected<std::shared_ptr<Cast>, std::string> fromJson(const nlohmann::json &jObj);

  const std::shared_ptr<Expression> &getExpr() const;

private:
  ::gandiva::NodePtr buildGandivaExpression();

  std::shared_ptr<Expression> expr_;
  std::shared_ptr<arrow::DataType> dataType_;

// caf inspect
public:
  template <class Inspector>
  friend bool inspect(Inspector& f, Cast& expr) {
    auto dataTypeToBytes = [&expr]() -> decltype(auto) {
      return fpdb::tuple::ArrowSerializer::dataType_to_bytes(expr.dataType_);
    };
    auto dataTypeFromBytes = [&expr](const std::vector<std::uint8_t> &bytes) {
      expr.dataType_ = fpdb::tuple::ArrowSerializer::bytes_to_dataType(bytes);
      return true;
    };
    return f.object(expr).fields(f.field("type", expr.type_),
                                 f.field("expr", expr.expr_),
                                 f.field("dataType", dataTypeToBytes, dataTypeFromBytes));
  }
};

std::shared_ptr<Expression> cast(const std::shared_ptr<Expression>& expr,
                                 const std::shared_ptr<arrow::DataType>& type);

}

#endif //FPDB_FPDB_EXPRESSION_GANDIVA_INCLUDE_FPDB_EXPRESSION_GANDIVA_CAST_H
