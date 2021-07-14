//
// Created by matt on 30/4/20.
//

#ifndef NORMAL_NORMAL_PUSHDOWN_INCLUDE_NORMAL_PUSHDOWN_JOIN_JOINER_H
#define NORMAL_NORMAL_PUSHDOWN_INCLUDE_NORMAL_PUSHDOWN_JOIN_JOINER_H

#include <memory>

#include <normal/tuple/TupleSet.h>

#include <arrow/api.h>

#include "normal/pushdown/join/ATTIC/HashTable.h"

namespace normal::pushdown::join {

class Joiner {

public:
  Joiner(JoinPredicate Pred,
		 std::shared_ptr<HashTable> Hashtable,
		 std::shared_ptr<normal::tuple::TupleSet2> Tuples);

  tl::expected<std::shared_ptr<normal::tuple::TupleSet2>, std::string> join();

private:

  /**
   * The join predicate
   */
  JoinPredicate pred_;

  /**
   * The hashed build relation
   */
  std::shared_ptr<HashTable> hashtable_;

  /**
   * The probe relation
   */
  std::shared_ptr<normal::tuple::TupleSet2> tuples_;

  [[nodiscard]] std::shared_ptr<normal::tuple::Schema> buildJoinedSchema() const;
  [[nodiscard]] tl::expected<std::shared_ptr<normal::tuple::TupleSet2>, std::string> processProbeTuples();

};

}
#endif //NORMAL_NORMAL_PUSHDOWN_INCLUDE_NORMAL_PUSHDOWN_JOIN_JOINER_H
