//
// Created by matt on 12/12/19.
//

#ifndef NORMAL_NORMAL_NORMAL_CORE_SRC_TUPLESET_H
#define NORMAL_NORMAL_NORMAL_CORE_SRC_TUPLESET_H

#include <memory>
#include <vector>

#include <arrow/api.h>
#include <arrow/csv/api.h>

namespace arrow { class Table; }
namespace arrow::csv { class TableReader; }

namespace normal::core {

class TupleSet {
private:
  std::shared_ptr<arrow::Table> table_;

public:
  static std::shared_ptr<TupleSet> make(const std::shared_ptr<arrow::csv::TableReader> &tableReader);
  static std::shared_ptr<TupleSet> make(std::shared_ptr<arrow::Table> table);

  int64_t numRows();
  std::string visit(std::string (*fn)(std::string, arrow::RecordBatch &));
  void addColumn(const std::string &name, int position, std::vector<std::shared_ptr<std::string>> data);

  std::string toString();

  [[nodiscard]] std::shared_ptr<arrow::Table> table() const;
  void table(const std::shared_ptr<arrow::Table> &table);
  std::string getValue(const std::string &columnName, int row);
  int64_t numColumns();

};

}

#endif //NORMAL_NORMAL_NORMAL_CORE_SRC_TUPLESET_H
