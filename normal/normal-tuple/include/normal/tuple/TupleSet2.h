//
// Created by matt on 1/5/20.
//

#ifndef NORMAL_NORMAL_TUPLE_INCLUDE_NORMAL_TUPLE_TUPLESET2_H
#define NORMAL_NORMAL_TUPLE_INCLUDE_NORMAL_TUPLE_TUPLESET2_H

#include <vector>

#include <arrow/api.h>
#include <arrow/table.h>

#include <normal/tuple/TupleSet.h>

#include "Column.h"
#include "Schema.h"
#include "TupleSetShowOptions.h"
#include "ColumnName.h"

namespace normal::tuple {

class TupleSet2 : public std::enable_shared_from_this<TupleSet2> {

public:

  /**
   * Creates an empty tuple set
   */
  explicit TupleSet2();

  /**
   * Creates a tuple set from an arrow table
   *
   * FIXME: Should add a validation call to the table
   *
   * @param arrowTable
   */
  explicit TupleSet2(std::shared_ptr<::arrow::Table> arrowTable);

  /**
   * Creates a tuple set from a v1 tuple set
   *
   * @param tuples
   * @return
   */
  static std::shared_ptr<TupleSet2> create(const std::shared_ptr<TupleSet>& tuples);

  /**
   * Creates a tuple set from an arrow table
   *
   * @param tuples
   * @return
   */
  static std::shared_ptr<TupleSet2> make(const std::shared_ptr<::arrow::Table> arrowTable);

  static std::shared_ptr<TupleSet2> make(const std::vector<std::shared_ptr<Column>>& columns);

  /**
   * Creates an empty tuple set with the given schema
   *
   * @param tuples
   * @return
   */
  static std::shared_ptr<TupleSet2> make(const std::shared_ptr<Schema>& schema);

  static std::shared_ptr<TupleSet2> make(const std::shared_ptr<Schema>& schema, const std::vector<std::shared_ptr<Column>>& columns);

  static std::shared_ptr<TupleSet2> make(const std::shared_ptr<::arrow::Schema>& schema, const std::vector<std::shared_ptr<::arrow::Array>>& arrays);

  static std::shared_ptr<TupleSet2> make(const std::shared_ptr<::arrow::Schema>& schema, const std::vector<std::shared_ptr<::arrow::ChunkedArray>>& arrays);

  /**
   * Creates an empty tupleset
   * @return
   */
  static std::shared_ptr<TupleSet2> make();

  /**
   * Creates an empty tupleset, but with an empty table (0x0)
   * @return
   */
  static std::shared_ptr<TupleSet2> make2();

  /**
   * Gets the tuple set as a v1 tuple set
   * @return
   */
  std::shared_ptr<TupleSet> toTupleSetV1();

  /**
   * Returns number of rows in the tuple set
   * @return
   */
  long numRows() const;

  /**
   * Returns number of columns in the tuple set
   * @return
   */
  long numColumns() const;

  /**
   * Clears the schema and all data
   */
  void clear();

  /**
   * Concatenates a vector of tuple sets into a new single tupleset
   *
   * @param tuples
   * @return
   */
  [[ nodiscard ]] static tl::expected<std::shared_ptr<TupleSet2>, std::string> concatenate(const std::vector<std::shared_ptr<TupleSet2>>& tupleSets);

  /**
   * Appends a vector of tuple sets to this tuple set and returns the new tuple set
   *
   * @param tuples
   * @return
   */
  [[ nodiscard ]] tl::expected<void, std::string> append(const std::vector<std::shared_ptr<TupleSet2>>& tupleSet);

  /**
   * Appends a tuple set to this tuple set and returns the new tuple set
   *
   * @param tuples
   * @return
   */
  [[ nodiscard ]] tl::expected<void, std::string> append(const std::shared_ptr<TupleSet2>& tupleSet);

  /**
   * Returns a single column by name
   *
   * @return
   */
  [[ nodiscard ]] tl::expected<std::shared_ptr<Column>, std::string> getColumnByName(const std::string &name);

  [[ nodiscard ]] tl::expected<std::shared_ptr<Column>, std::string> getColumnByIndex(const int &columnIndex);

  /**
   * Returns the tuple set pretty printed as a string
   *
   * @return
   */
  std::string showString();
  std::string showString(TupleSetShowOptions options);

  /**
   * Returns a short string representing the tuple set
   * @return
   */
  std::string toString() const;

  /**
   * Gets the tuple set schema
   *
   * @return The tuple set schema or nullopt if not yet defined
   */
  std::optional<std::shared_ptr<Schema>> schema() const;

  /**
   * Sets the tuple set schema
   *
   * TODO: This is a destructive operation which just overwrites the table
   *  Could perhaps do a check to see if any columns can be preserved across schema changes.
   *  Or, maybe its just better to make at least some of the tuple set immutable?
   */
  void setSchema(const std::shared_ptr<Schema>& Schema);

  const std::optional<std::shared_ptr<::arrow::Table>> &getArrowTable() const;

  bool validate();

  tl::expected<std::string, std::string> getString(const std::string &columnName, int row);


  tl::expected<void, std::string> renameColumns(const std::vector<std::string>& columnNames){

    if(table_.has_value()){
      auto result = table_.value()->RenameColumns(columnNames, &table_.value());
      if(!result.ok())
        return tl::make_unexpected(result.message());
    }

    return {};
  }


private:

  /**
   * The underlying arrow table, which may or may not be set to support an "empty" tuple set
   */
  std::optional<std::shared_ptr<::arrow::Table>> table_;

  static std::vector<std::shared_ptr<arrow::Table>> tupleSetVectorToArrowTableVector(const std::vector<std::shared_ptr<TupleSet2>> &tupleSets);

};

}

#endif //NORMAL_NORMAL_TUPLE_INCLUDE_NORMAL_TUPLE_TUPLESET2_H
