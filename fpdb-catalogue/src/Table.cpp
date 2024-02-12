//
// Created by Yifei Yang on 11/8/21.
//

#include <fpdb/catalogue/Table.h>
#include <fmt/format.h>
#include <utility>

namespace fpdb::catalogue {

Table::Table(string name,
             const shared_ptr<arrow::Schema>& schema,
             const shared_ptr<fpdb::tuple::FileFormat>& format,
             const unordered_map<string, int> &apxColumnLengthMap,
             int apxRowLength,
             const unordered_set<string> &zonemapColumnNames) :
  name_(std::move(name)),
  schema_(schema),
  format_(format),
  apxColumnLengthMap_(apxColumnLengthMap),
  apxRowLength_(apxRowLength),
  zonemapColumnNames_(zonemapColumnNames) {}

const string &Table::getName() const {
  return name_;
}

const shared_ptr<arrow::Schema> &Table::getSchema() const {
  return schema_;
}

const shared_ptr<fpdb::tuple::FileFormat> &Table::getFormat() const {
  return format_;
}

vector<string> Table::getColumnNames() const {
  return schema_->field_names();
}

int Table::getApxColumnLength(const string &columnName) const {
  const auto &it = apxColumnLengthMap_.find(columnName);
  if (it == apxColumnLengthMap_.end()) {
    throw runtime_error(fmt::format("Column {} not found in Table {}.", columnName, name_));
  }
  return it->second;
}

int Table::getApxRowLength() const {
  return apxRowLength_;
}

}
