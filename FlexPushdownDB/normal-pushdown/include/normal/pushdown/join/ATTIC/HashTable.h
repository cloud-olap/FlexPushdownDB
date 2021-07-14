//
// Created by matt on 30/4/20.
//

#ifndef NORMAL_NORMAL_PUSHDOWN_INCLUDE_NORMAL_PUSHDOWN_JOIN_HASHTABLE_H
#define NORMAL_NORMAL_PUSHDOWN_INCLUDE_NORMAL_PUSHDOWN_JOIN_HASHTABLE_H

#include <memory>
#include <unordered_map>

#include <arrow/scalar.h>
#include <arrow/record_batch.h>
#include <arrow/table.h>
#include <map>
#include <normal/tuple/TupleSet2.h>

#include "normal/pushdown/join/JoinPredicate.h"

using namespace normal::tuple;

namespace normal::pushdown::join {

struct ScalarPointerHash {
  inline size_t operator()(const std::shared_ptr<Scalar> &scalar) const {
	return scalar->hash();
  }
};

struct ScalarPointerPredicate {
  inline bool operator()(const std::shared_ptr<Scalar>& lhs, const std::shared_ptr<Scalar>& rhs) const {
	return *lhs == *rhs;
  }
};

typedef std::unordered_multimap<std::shared_ptr<Scalar>, long, ScalarPointerHash, ScalarPointerPredicate> ValueRowMap;


/**
 * The hashtable data structure used in a hash join.
 *
 * Contains a mapping of join column values to the rows the value appears in.
 *
 * Since arrow is column oriented, the hashtable doesn't map to the rows in the table directly, but
 * maps to the row number the value is found in.
 *
 * This may only represent a partial mapping as tuples are joined in batches, so should support merging of hash tables.
 */
class HashTable {

public:
  HashTable();

  [[nodiscard]] const std::shared_ptr<TupleSet2> &getTupleSet() const;
  [[nodiscard]] const std::shared_ptr<ValueRowMap> &getValueRowMap() const;

  void clear();
  void merge(const std::shared_ptr<HashTable> &other);
  [[nodiscard]] tl::expected<void, std::string> put(const std::string &columnName, const std::shared_ptr<TupleSet2> &tupleSet);
  std::string toString();
  size_t size();

private:

  /**
   * The (ideally smaller) table being joined
   */
  std::shared_ptr<TupleSet2> tuples_;

  /**
   * Mapping from join column values to rows in join table
   */
  std::shared_ptr<ValueRowMap> valueIndexMap_;

};

}

#endif //NORMAL_NORMAL_PUSHDOWN_INCLUDE_NORMAL_PUSHDOWN_JOIN_HASHTABLE_H
