//
// Created by matt on 1/5/20.
//

#include "normal/tuple/TupleSet2.h"

#include <utility>

#include <arrow/api.h>
#include <arrow/pretty_print.h>
#include <iomanip>

#include <normal/tuple/arrow/TableHelper.h>
#include <normal/tuple/arrow/Arrays.h>
#include <string>
#include <sstream>
#include <iostream>

using namespace normal::tuple;

TupleSet2::TupleSet2() :
	table_(std::nullopt) {
}

TupleSet2::TupleSet2(std::shared_ptr<::arrow::Table> arrowTable) :
	table_(std::optional(std::move(arrowTable))) {
}

std::shared_ptr<TupleSet2> TupleSet2::create(const std::shared_ptr<TupleSet> &tuples) {
  return std::make_shared<TupleSet2>(tuples->table());
}

void TupleSet2::clear() {
  table_ = std::nullopt;
}

long TupleSet2::numRows() const {
  if (table_)
	return table_.value()->num_rows();
  else
	return 0;
}

tl::expected<std::shared_ptr<TupleSet2>,
			 std::string> TupleSet2::concatenate(const std::vector<std::shared_ptr<TupleSet2>> &tupleSets) {

  // Make sure the tuple sets are valid and have the same schema
  std::shared_ptr<TupleSet2> tupleSet1;
  for(const auto& tupleSet: tupleSets){
	if(!tupleSet->getArrowTable().has_value()){
	  return tl::make_unexpected("Cannot concatenate empty tuple sets");
	}
    if(tupleSet1 == nullptr){
	  tupleSet1 = tupleSet;
    }
    else{
      if(!tupleSet->getArrowTable().value()->schema()->Equals(tupleSet1->getArrowTable().value()->schema())){
        return tl::make_unexpected("Cannot concatenate tuple sets with different schemas");
      }
    }
  }

  // Create a vector of arrow tables to concatenate
  std::vector<std::shared_ptr<::arrow::Table>> tableVector = tupleSetVectorToArrowTableVector(tupleSets);

  // Concatenate
  auto tableResult = arrow::ConcatenateTables(tableVector);
  if (tableResult.ok()) {
	return std::make_shared<TupleSet2>(*tableResult);
  } else {
	return tl::make_unexpected(tableResult.status().ToString());
  }
}

std::vector<std::shared_ptr<arrow::Table>> TupleSet2::tupleSetVectorToArrowTableVector(const std::vector<std::shared_ptr<
	TupleSet2>> &tupleSets) {

  std::vector<std::shared_ptr<arrow::Table>> tableVector;
  tableVector.reserve(tupleSets.size());
  for (const auto &tupleSet: tupleSets) {
	if (tupleSet->table_.has_value())
	  tableVector.push_back(tupleSet->table_.value());
  }

  return tableVector;
}

tl::expected<void, std::string> TupleSet2::append(const std::vector<std::shared_ptr<TupleSet2>> &tupleSets) {
  auto tupleSetVector = std::vector{shared_from_this()};
  tupleSetVector.insert(tupleSetVector.end(), tupleSets.begin(), tupleSets.end());
  auto expected = concatenate(tupleSetVector);
  if (!expected.has_value())
	return tl::make_unexpected(expected.error());
  else {
	this->table_ = expected.value()->table_;
	return {}; // TODO: This seems to be how to return a void expected AFAICT, verify?
  }
}

tl::expected<void, std::string> TupleSet2::append(const std::shared_ptr<TupleSet2> &tupleSet) {
  auto tupleSetVector = std::vector{tupleSet};
  return append(tupleSetVector);
}

std::string TupleSet2::showString() {
  return showString(TupleSetShowOptions(TupleSetShowOrientation::ColumnOriented));
}

std::shared_ptr<TupleSet> TupleSet2::toTupleSetV1() {

  // FIXME: V1 tuple sets cant be empty

  return TupleSet::make(table_.value());
}

long TupleSet2::numColumns() const {
  if (table_.has_value()) {
	return table_.value()->num_columns();
  } else {
	return 0;
  }
}

std::string TupleSet2::showString(TupleSetShowOptions options) {

  if (!table_.has_value()) {
	return "<empty>";
  } else {

	auto table = table_.value();

	if (options.getOrientation() == TupleSetShowOrientation::ColumnOriented) {
	  auto ss = std::stringstream();
	  arrow::Status arrowStatus = ::arrow::PrettyPrint(*table, 0, &ss);

	  if (!arrowStatus.ok()) {
		// FIXME: How to handle this? How can pretty print fail? RAM? Maybe just abort?
		throw std::runtime_error(arrowStatus.detail()->ToString());
	  }

	  return ss.str();
	} else {

	  int width = 120;

	  std::stringstream ss;

	  ss << std::endl;

	  ss << std::left << std::setw(width) << std::setfill('-') << "" << std::endl;

	  int columnWidth = table->num_columns() == 0 ? width : width / table->num_columns();

	  // Column names
	  for (const auto &field: table_.value()->schema()->fields()) {
		ss << std::left << std::setw(columnWidth) << std::setfill(' ');
		ss << "| " + field->name();
	  }
	  ss << std::endl;

	  // Column types
	  for (const auto &field: table_.value()->schema()->fields()) {
		ss << std::left << std::setw(columnWidth) << std::setfill(' ');
		ss << "| " + field->type()->ToString();
	  }
	  ss << std::endl;

	  ss << std::left << std::setw(width) << std::setfill('-') << "" << std::endl;

	  // Data
	  int rowCounter = 0;
	  for (int rowIndex = 0; rowIndex < table_.value()->num_rows(); ++rowIndex) {

		if (rowCounter >= options.getMaxNumRows())
		  break;

		for (int columnIndex = 0; columnIndex < table_.value()->num_columns(); ++columnIndex) {
		  auto column = this->getColumnByIndex(columnIndex).value();
		  auto value = column->element(rowIndex).value();
		  ss << std::left << std::setw(columnWidth) << std::setfill(' ');
		  ss << "| " + value->toString();
		}
		ss << std::endl;

		rowCounter++;
	  }

	  // Shape
	  ss << std::left << std::setw(width) << std::setfill('-') << "" << std::endl;
	  ss << table->num_columns() << " cols x " << table->num_rows() << " rows";
	  if (rowCounter < table->num_rows()) {
		ss << " (showing only top " << rowCounter << " rows)";
	  }

	  ss << std::endl;

	  return ss.str();
	}
  }
}

std::string TupleSet2::toString() const {
  if(table_.has_value())
  	return fmt::format("<TupleSet2: {} x {}>", numColumns(), numRows());
  else
	return fmt::format("<TupleSet2: empty>", numColumns(), numRows());
}

const std::optional<std::shared_ptr<::arrow::Table>> &TupleSet2::getArrowTable() const {
  return table_;
}

std::shared_ptr<TupleSet2> TupleSet2::make(const std::shared_ptr<Schema> &Schema) {
  auto columns = Schema->makeColumns();
  return make(Schema, columns);
}

std::shared_ptr<TupleSet2> TupleSet2::make(const std::shared_ptr<Schema> &Schema,
										   const std::vector<std::shared_ptr<Column>> &columns) {
  auto chunkedArrays = Column::columnVectorToArrowChunkedArrayVector(columns);
  auto arrowTable = ::arrow::Table::Make(Schema->getSchema(), chunkedArrays);
  auto tupleSet = std::make_shared<TupleSet2>(arrowTable);
  return tupleSet;
}

std::shared_ptr<TupleSet2> TupleSet2::make() {
  auto tupleSet = std::make_shared<TupleSet2>();
  return tupleSet;
}

tl::expected<std::shared_ptr<Column>, std::string> TupleSet2::getColumnByName(const std::string &name) {

  auto canonicalColumnName = ColumnName::canonicalize(name);

  auto columnArray = table_.value()->GetColumnByName(canonicalColumnName);
  if (columnArray == nullptr) {
	return tl::make_unexpected("Column '" + canonicalColumnName + "' does not exist");
  } else {
	auto column = Column::make(canonicalColumnName, columnArray);
	return column;
  }
}

tl::expected<std::shared_ptr<Column>, std::string> TupleSet2::getColumnByIndex(const int &columnIndex) {
  auto columnName = table_.value()->field(columnIndex)->name();
  auto columnArray = table_.value()->column(columnIndex);
  if (columnArray == nullptr) {
	return tl::make_unexpected("Column '" + std::to_string(columnIndex) + "' does not exist");
  } else {
	auto column = Column::make(columnName, columnArray);
	return column;
  }
}

std::optional<std::shared_ptr<Schema>> TupleSet2::schema() const {
  if(table_.has_value()) {
	return std::make_shared<Schema>(table_.value()->schema());
  }
  else{
	return std::nullopt;
  }
}

void TupleSet2::setSchema(const std::shared_ptr<Schema> &Schema) {
  auto columns = Schema->makeColumns();
  auto chunkedArrays = Column::columnVectorToArrowChunkedArrayVector(columns);
  auto arrowTable = ::arrow::Table::Make(Schema->getSchema(), chunkedArrays);
  table_ = std::optional(arrowTable);
}

bool TupleSet2::validate() {
  if(table_.has_value())
	return table_.value()->ValidateFull().ok();
  else
	return true;
}

std::shared_ptr<TupleSet2> TupleSet2::make(std::shared_ptr<::arrow::Table> arrowTable) {
  return std::make_shared<TupleSet2>(std::move(arrowTable));
}

std::shared_ptr<TupleSet2> TupleSet2::make(const std::shared_ptr<::arrow::Schema> schema, const std::vector<std::shared_ptr<::arrow::Array>>& arrays) {
  auto arrowTable = ::arrow::Table::Make (schema, arrays);
  return std::make_shared<TupleSet2>(std::move(arrowTable));
}

std::shared_ptr<TupleSet2> TupleSet2::make(std::shared_ptr<::arrow::Schema> schema,
										   const std::vector<std::shared_ptr<::arrow::ChunkedArray>> &arrays) {
  auto arrowTable = ::arrow::Table::Make (schema, arrays);
  return std::make_shared<TupleSet2>(std::move(arrowTable));
}

tl::expected<std::string, std::string> TupleSet2::getString(const std::string &columnName, int row){
  if(!table_.has_value()){
    return tl::unexpected(fmt::format("Column with name '{}' not found", columnName));
  }
  else {
	return TableHelper::value<::arrow::StringType, std::string>(columnName, row, *table_.value());
  }
}

std::shared_ptr<TupleSet2> TupleSet2::make(std::vector<std::shared_ptr<Column>> columns) {

  std::vector<std::shared_ptr<::arrow::ChunkedArray>> arrays;
  std::vector<std::shared_ptr<::arrow::Field>> fields;

  for(const auto &column: columns){
	arrays.push_back(column->getArrowArray());
	fields.push_back(std::make_shared<::arrow::Field>(column->getName(), column->type()));
  }

  auto schema = std::make_shared<::arrow::Schema>(fields);

  return make (schema, arrays);
}
