//
// Created by Yifei Yang on 11/21/21.
//

#ifndef FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_PRUNE_PARTITIONPRUNER_H
#define FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_PRUNE_PARTITIONPRUNER_H

#include <fpdb/catalogue/Partition.h>
#include <fpdb/expression/gandiva/Expression.h>
#include <fpdb/tuple/Scalar.h>

using namespace fpdb::catalogue;
using namespace fpdb::expression::gandiva;
using namespace fpdb::tuple;

namespace fpdb::executor::physical {

class PartitionPruner {

public:
  static unordered_map<shared_ptr<Partition>, shared_ptr<Expression>, PartitionPointerHash, PartitionPointerPredicate>
  prune(const vector<shared_ptr<Partition>> &partitions, const shared_ptr<Expression> &predicate);

private:
  /**
   * Prune predicate given min max value on a specific column, the predicate should be canonicalized
   * @param predicate
   * @param columnName
   * @param min
   * @param max
   * @return
   */
  static shared_ptr<Expression> prunePredicate(const shared_ptr<Expression> &predicate, const string &columnName,
                                               const shared_ptr<Scalar> &statsMin, const shared_ptr<Scalar> &statsMax);

  static shared_ptr<Scalar> litToScalar(const shared_ptr<Expression> &literal);

  static bool checkValid(const shared_ptr<Scalar> &predMin, const shared_ptr<Scalar> &predMax,
                         bool predMinOpen, bool predMaxOpen,
                         const shared_ptr<Scalar> &min, const shared_ptr<Scalar> &max);

  static bool lt(const shared_ptr<Scalar> &v1, const shared_ptr<Scalar> &v2);
  static bool lte(const shared_ptr<Scalar> &v1, const shared_ptr<Scalar> &v2);

};

}

#endif //FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_PRUNE_PARTITIONPRUNER_H
