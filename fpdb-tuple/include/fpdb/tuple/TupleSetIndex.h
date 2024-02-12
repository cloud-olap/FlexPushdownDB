//
// Created by matt on 1/8/20.
//

#ifndef FPDB_FPDB_TUPLE_INCLUDE_FPDB_TUPLE_TUPLESETINDEX_H
#define FPDB_FPDB_TUPLE_INCLUDE_FPDB_TUPLE_TUPLESETINDEX_H

#include <fpdb/tuple/TupleKey.h>
#include <fpdb/tuple/TupleSet.h>
#include <fpdb/caf/CAFUtil.h>
#include <arrow/api.h>
#include <tl/expected.hpp>
#include <utility>
#include <vector>
#include <unordered_map>

using namespace std;

namespace fpdb::tuple {

/**
 * An index into a tuple set
 *
 * Contains a map of values for particular columns to row numbers in the tuple set
 *
 * TODO: Should really operate on tuple sets and not an arrow table
 */
class TupleSetIndex {
public:
  TupleSetIndex(vector<string> columnNames,
                vector<int> columnIndexes,
                shared_ptr<TupleSet> tupleSet,
                unordered_multimap<shared_ptr<TupleKey>, int64_t, TupleKeyPointerHash, TupleKeyPointerPredicate> valueRowMap);
  TupleSetIndex() = default;
  TupleSetIndex(const TupleSetIndex&) = default;
  TupleSetIndex& operator=(const TupleSetIndex&) = default;
  virtual ~TupleSetIndex() = default;

  static tl::expected<shared_ptr<TupleSetIndex>, string> make(const vector<string> &columnNames,
                                                              const shared_ptr<TupleSet> &tupleSet);

  int64_t size() const;
  const shared_ptr<TupleSet> &getTupleSet() const;
  string toString() const;

  /**
   * Invokes combineChunks on the underlying value->row map
   *
   * @return
   */
  tl::expected<void, string> combine();

  /**
   * Build TupleSetIndex given columnIndexes, row offset and table
   *
   * @param columnIndexes
   * @param rowIndexOffset
   * @param table
   * @return
   */
  static tl::expected<unordered_multimap<shared_ptr<TupleKey>, int64_t, TupleKeyPointerHash, TupleKeyPointerPredicate>, string>
  build(const vector<int>& columnIndexes, int64_t rowIndexOffset, const std::shared_ptr<::arrow::Table> &table);

  /**
   * Adds the given table to the index
   *
   * @param table
   * @return
   */
  tl::expected<void, string> put(const shared_ptr<::arrow::Table> &table);

  /**
   * Adds another index to this index
   *
   * @param other
   * @return
   */
  tl::expected<void, string> merge(const shared_ptr<TupleSetIndex> &other);

  /**
   * Find rows of given tupleKey
   *
   * @param tupleKey
   * @return
   */
  vector<int64_t> find(const shared_ptr<TupleKey> &tupleKey);
  
  /**
   * Validate the tupleSetIndex
   * @return 
   */
  tl::expected<void, string> validate();

private:
  vector<string> columnNames_;
  vector<int> columnIndexes_;
  shared_ptr<TupleSet> tupleSet_;
  unordered_multimap<shared_ptr<TupleKey>, int64_t, TupleKeyPointerHash, TupleKeyPointerPredicate> valueRowMap_;

// caf inspect
public:
  template <class Inspector>
  friend bool inspect(Inspector& f, TupleSetIndex& tupleSetIndex) {
    return f.object(tupleSetIndex).fields(f.field("columnNames", tupleSetIndex.columnNames_),
                                          f.field("columnIndexes", tupleSetIndex.columnIndexes_),
                                          f.field("tupleSet", tupleSetIndex.tupleSet_),
                                          f.field("valueRowMap", tupleSetIndex.valueRowMap_));
  }
};

}

using TupleSetIndexPtr = std::shared_ptr<fpdb::tuple::TupleSetIndex>;

CAF_BEGIN_TYPE_ID_BLOCK(TupleSetIndex, fpdb::caf::CAFUtil::TupleSetIndex_first_custom_type_id)
CAF_ADD_TYPE_ID(TupleSetIndex, (fpdb::tuple::TupleSetIndex))
CAF_END_TYPE_ID_BLOCK(TupleSetIndex)

namespace caf {
template <>
struct inspector_access<TupleSetIndexPtr> : variant_inspector_access<TupleSetIndexPtr> {
  // nop
};
} // namespace caf

#endif //FPDB_FPDB_TUPLE_INCLUDE_FPDB_TUPLE_TUPLESETINDEX_H
