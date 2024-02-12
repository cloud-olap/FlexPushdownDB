//
// Created by matt on 27/4/20.
//

#include "fpdb/expression/gandiva/Cast.h"
#include "fpdb/expression/gandiva/Column.h"
#include "fpdb/expression/gandiva/Projector.h"
#include <fpdb/tuple/serialization/ArrowSerializer.h>
#include <fpdb/tuple/Column.h>
#include <fmt/format.h>
#include <utility>

#include <gandiva/tree_expr_builder.h>

using namespace fpdb::expression::gandiva;
using namespace fpdb::tuple;

Cast::Cast(std::shared_ptr<Expression> expr, std::shared_ptr<arrow::DataType> dataType) :
  Expression(CAST),
	expr_(std::move(expr)), dataType_(std::move(dataType)) {}

tl::expected<std::shared_ptr<TupleSet>, std::string>
Cast::castDate32ToDate64(const std::shared_ptr<TupleSet> &tupleSet) {
  std::vector<std::shared_ptr<fpdb::tuple::Column>> resultColumns, columnsToConvert;

  // separate date32 columns out
  for (int c = 0; c < tupleSet->numColumns(); ++c) {
    auto expColumn = tupleSet->getColumnByIndex(c);
    if (!expColumn.has_value()) {
      return tl::make_unexpected(expColumn.error());
    }
    auto column = *expColumn;

    if (column->type()->id() == ::arrow::date32()->id()) {
      columnsToConvert.emplace_back(column);
      resultColumns.emplace_back(nullptr);
    } else {
      resultColumns.emplace_back(column);
    }
  }

  // no date32 columns
  if (columnsToConvert.empty()) {
    return tupleSet;
  }

  // convert
  std::vector<std::shared_ptr<Expression>> exprs;
  for (const auto &column: columnsToConvert) {
    exprs.emplace_back(cast(col(column->getName()), ::arrow::date64()));
  }
  auto subTupleSetToConvert = TupleSet::make(columnsToConvert);

  auto projector = std::make_shared<Projector>(exprs);
  auto result = projector->compile(subTupleSetToConvert->schema());
  if (!result.has_value()) {
    return tl::make_unexpected(result.error());
  }

  auto expConvertedSubTupleSet = projector->evaluate(*subTupleSetToConvert);
  if (!expConvertedSubTupleSet.has_value()) {
    return tl::make_unexpected(expConvertedSubTupleSet.error());
  }
  auto convertedSubTupleSet = *expConvertedSubTupleSet;
  convertedSubTupleSet->renameColumns(subTupleSetToConvert->schema()->field_names());

  // make result tupleSet
  int convertedColumnId = 0;
  for (uint c = 0; c < resultColumns.size(); ++c) {
    if (!resultColumns[c]) {
      auto expConvertedColumn = convertedSubTupleSet->getColumnByIndex(convertedColumnId++);
      if (!expConvertedColumn.has_value()) {
        return tl::make_unexpected(expConvertedSubTupleSet.error());
      }
      resultColumns[c] = *expConvertedColumn;
    }
  }

  return TupleSet::make(resultColumns);
}

::gandiva::NodePtr Cast::buildGandivaExpression() {

  auto paramGandivaExpression = expr_->getGandivaExpression();
  auto fromArrowType = expr_->getReturnType();

  /**
   * NOTE: Some cast operations are not supported by Gandiva so we set up some special cases here
   */
  if (fromArrowType->id() == arrow::utf8()->id() && dataType_->id() == arrow::float64()->id()) {
    // Not supported directly by Gandiva, need to cast string to decimal and then that to float64

    auto castDecimalFunctionName = "castDECIMAL";
    auto castDecimalReturnType = arrow::decimal(30, 8); // FIXME: Need to check if this is sufficient to cast to double
    auto castToDecimalFunction = ::gandiva::TreeExprBuilder::MakeFunction(castDecimalFunctionName,
                                        {paramGandivaExpression},
                                        castDecimalReturnType);

    auto castFunctionName = "castFloat8";

    auto castFunction = ::gandiva::TreeExprBuilder::MakeFunction(castFunctionName,
                                   {castToDecimalFunction},
                                   dataType_);

    return castFunction;
  }

  else if (fromArrowType->id() == arrow::utf8()->id() && dataType_->id() == arrow::int64()->id()) {
    // Not supported directly by Gandiva, need to cast string to decimal and then that to int64

    auto castDecimalFunctionName = "castDECIMAL";
    auto castDecimalReturnType = arrow::decimal(38, 0); // FIXME: Need to check if this is sufficient to cast to double
    auto castToDecimalFunction = ::gandiva::TreeExprBuilder::MakeFunction(castDecimalFunctionName,
                                        {paramGandivaExpression},
                                        castDecimalReturnType);

    auto castFunctionName = "castBIGINT";

    auto castFunction = ::gandiva::TreeExprBuilder::MakeFunction(castFunctionName,
                                   {castToDecimalFunction},
                                   dataType_);

    return castFunction;
  }

  else if (fromArrowType->id() == arrow::utf8()->id() && dataType_->id() == arrow::int32()->id()) {
    // Not supported directly by Gandiva, need to cast string to decimal to int64 and then that to int32

    auto castDecimalFunctionName = "castDECIMAL";
    auto castDecimalReturnType = arrow::decimal(38, 0); // FIXME: Need to check if this is sufficient to cast to double
    auto castToDecimalFunction = ::gandiva::TreeExprBuilder::MakeFunction(castDecimalFunctionName,
                                        {paramGandivaExpression},
                                        castDecimalReturnType);

    auto castToInt64Function = ::gandiva::TreeExprBuilder::MakeFunction("castBIGINT",
                                        {castToDecimalFunction},
                                        ::arrow::int64());

    auto castFunctionName = "castINT";

    auto castFunction = ::gandiva::TreeExprBuilder::MakeFunction(castFunctionName,
                                   {castToInt64Function},
                                   dataType_);

    return castFunction;
  }

  else if (fromArrowType->id() == arrow::date32()->id() && dataType_->id() == arrow::date64()->id()) {
    return ::gandiva::TreeExprBuilder::MakeFunction("castDATE", {paramGandivaExpression}, dataType_);
  }

  else {
    throw std::runtime_error(fmt::format("Unsupported cast from '{}' to '{}'", fromArrowType->name(), dataType_->name()));
  }
}

void Cast::compile(const std::shared_ptr<arrow::Schema> &schema) {
  expr_->compile(schema);

  gandivaExpression_ = buildGandivaExpression();
  returnType_ = dataType_;
}

std::string Cast::alias() {
  return expr_->alias();
}

std::set<std::string> Cast::involvedColumnNames() {
  return expr_->involvedColumnNames();
}

const std::shared_ptr<Expression> &Cast::getExpr() const {
  return expr_;
}

std::string Cast::getTypeString() const {
  return "Cast";
}

::nlohmann::json Cast::toJson() const {
  ::nlohmann::json jObj;
  jObj.emplace("type", getTypeString());
  jObj.emplace("expr", expr_->toJson());
  jObj.emplace("dataType", fpdb::tuple::ArrowSerializer::dataType_to_bytes(dataType_));
  return jObj;
}

tl::expected<std::shared_ptr<Cast>, std::string> Cast::fromJson(const nlohmann::json &jObj) {
  if (!jObj.contains("expr")) {
    return tl::make_unexpected(fmt::format("Expr not specified in Cast expression JSON '{}'", to_string(jObj)));
  }
  auto expExpr = Expression::fromJson(jObj["expr"]);
  if (!expExpr.has_value()) {
    return tl::make_unexpected(expExpr.error());
  }

  if (!jObj.contains("dataType")) {
    return tl::make_unexpected(fmt::format("DataType not specified in Cast expression JSON '{}'", to_string(jObj)));
  }
  auto dataType = fpdb::tuple::ArrowSerializer::bytes_to_dataType(jObj["dataType"].get<std::vector<uint8_t>>());

  return std::make_shared<Cast>(*expExpr, dataType);
}

std::shared_ptr<Expression> fpdb::expression::gandiva::cast(const std::shared_ptr<Expression>& expr,
                                                              const std::shared_ptr<arrow::DataType> &type) {
  return std::make_shared<Cast>(expr, type);
}
