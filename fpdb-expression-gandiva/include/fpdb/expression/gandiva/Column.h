//
// Created by matt on 27/4/20.
//

#ifndef FPDB_FPDB_EXPRESSION_GANDIVA_INCLUDE_FPDB_EXPRESSION_GANDIVA_COLUMN_H
#define FPDB_FPDB_EXPRESSION_GANDIVA_INCLUDE_FPDB_EXPRESSION_GANDIVA_COLUMN_H

#include <string>
#include <memory>

#include <arrow/api.h>
#include <gandiva/node.h>

#include "Expression.h"

namespace fpdb::expression::gandiva {

class Column : public Expression {

public:
  explicit Column(std::string columnName);
  Column() = default;
  Column(const Column&) = default;
  Column& operator=(const Column&) = default;

  void compile(const std::shared_ptr<arrow::Schema> &schema) override;
  std::string alias() override;
  std::string getTypeString() const override;
  std::set<std::string> involvedColumnNames() override;
  ::nlohmann::json toJson() const override;
  static tl::expected<std::shared_ptr<Column>, std::string> fromJson(const nlohmann::json &jObj);

  [[nodiscard]] const std::string &getColumnName() const;

private:
  std::string columnName_;

// caf inspect
public:
  template <class Inspector>
  friend bool inspect(Inspector& f, Column& expr) {
    return f.object(expr).fields(f.field("type", expr.type_),
                                 f.field("columnName", expr.columnName_));
  }
};

std::shared_ptr<Expression> col(const std::string& columnName);

}

#endif //FPDB_FPDB_EXPRESSION_GANDIVA_INCLUDE_FPDB_EXPRESSION_GANDIVA_COLUMN_H
