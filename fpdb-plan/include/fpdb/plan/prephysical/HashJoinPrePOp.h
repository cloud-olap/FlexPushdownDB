//
// Created by Yifei Yang on 11/7/21.
//

#ifndef FPDB_FPDB_PLAN_INCLUDE_FPDB_PLAN_PREPHYSICAL_HASHJOINPREPOP_H
#define FPDB_FPDB_PLAN_INCLUDE_FPDB_PLAN_PREPHYSICAL_HASHJOINPREPOP_H

#include <fpdb/plan/prephysical/PrePhysicalOp.h>
#include <fpdb/plan/prephysical/JoinType.h>

namespace fpdb::plan::prephysical {

class HashJoinPrePOp: public PrePhysicalOp {
public:
  // column names for evalauting expressions prior to the hash join
  constexpr static const char *const HASH_JOIN_LEFT_INPUT_EXPR_PREFIX = "HASH_JOIN_LEFT_INPUT_EXPR_";
  constexpr static const char *const HASH_JOIN_RIGHT_INPUT_EXPR_PREFIX = "HASH_JOIN_RIGHT_INPUT_EXPR_";

  HashJoinPrePOp(uint id,
                 double rowCount,
                 JoinType joinType,
                 const vector<string> &leftColumnNames,
                 const vector<string> &rightColumnNames,
                 bool pushable);

  string getTypeString() override;
  set<string> getUsedColumnNames() override;

  void switchSide();

  JoinType getJoinType() const;
  const vector<string> &getLeftColumnNames() const;
  const vector<string> &getRightColumnNames() const;
  bool isPushable() const;
  int getNumJoinColumnPairs() const;

private:
  bool equalTo(const std::shared_ptr<PrePhysicalOp> &other) const override;

  JoinType joinType_;
  vector<string> leftColumnNames_;
  vector<string> rightColumnNames_;
  bool pushable_;
};

}


#endif //FPDB_FPDB_PLAN_INCLUDE_FPDB_PLAN_PREPHYSICAL_HASHJOINPREPOP_H
