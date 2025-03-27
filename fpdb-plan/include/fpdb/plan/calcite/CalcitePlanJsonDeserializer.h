//
// Created by Yifei Yang on 11/1/21.
//

#ifndef FPDB_FPDB_PLAN_INCLUDE_FPDB_PLAN_CALCITEPLANJSONDESERIALIZER_H
#define FPDB_FPDB_PLAN_INCLUDE_FPDB_PLAN_CALCITEPLANJSONDESERIALIZER_H

#include <fpdb/plan/prephysical/PrePhysicalPlan.h>
#include <fpdb/plan/prephysical/PrePhysicalOp.h>
#include <fpdb/plan/prephysical/SortPrePOp.h>
#include <fpdb/plan/prephysical/LimitSortPrePOp.h>
#include <fpdb/plan/prephysical/AggregatePrePOp.h>
#include <fpdb/plan/prephysical/GroupPrePOp.h>
#include <fpdb/plan/prephysical/ProjectPrePOp.h>
#include <fpdb/plan/prephysical/HashJoinPrePOp.h>
#include <fpdb/plan/prephysical/NestedLoopJoinPrePOp.h>
#include <fpdb/plan/prephysical/FilterPrePOp.h>
#include <fpdb/plan/prephysical/FilterableScanPrePOp.h>
#include <fpdb/plan/prephysical/UnionAllPrePOp.h>
#include <fpdb/catalogue/CatalogueEntry.h>
#include <nlohmann/json.hpp>
#include <string>

using namespace fpdb::plan::prephysical;
using json = nlohmann::json;
using namespace std;

namespace fpdb::plan::calcite {

class CalcitePlanJsonDeserializer {

public:
  static shared_ptr<PrePhysicalPlan> deserialize(string planJsonString,
                                                 const shared_ptr<CatalogueEntry> &catalogueEntry);

private:
  CalcitePlanJsonDeserializer(string planJsonString,
                              const shared_ptr<CatalogueEntry> &catalogueEntry);

  /**
   * Impl of deserialization
   * @return
   */
  shared_ptr<PrePhysicalPlan> deserialize();

  shared_ptr<PrePhysicalOp> deserializeDfs(json &jObj);
  vector<shared_ptr<PrePhysicalOp>> deserializeProducers(const json &jObj);

  // serialize common fields: <row count>
  std::tuple<double> deserializeCommon(const json &jObj);

  shared_ptr<fpdb::expression::gandiva::Expression> deserializeInputRef(const json &jObj);
  shared_ptr<fpdb::expression::gandiva::Expression> deserializeLiteral(const json &jObj);
  shared_ptr<fpdb::expression::gandiva::Expression> deserializeOperation(const json &jObj);
  shared_ptr<fpdb::expression::gandiva::Expression> deserializeAndOrNotOperation(const string &opName, const json &jObj);
  shared_ptr<fpdb::expression::gandiva::Expression> deserializeBinaryOperation(const string &opName, const json &jObj);
  shared_ptr<fpdb::expression::gandiva::Expression> deserializeInOperation(const json &jObj);
  shared_ptr<fpdb::expression::gandiva::Expression> deserializeCaseOperation(const json &jObj);
  shared_ptr<fpdb::expression::gandiva::Expression> deserializeExtractOperation(const json &jObj);
  shared_ptr<fpdb::expression::gandiva::Expression> deserializeNullOperation(const string &opName, const json &jObj);
  shared_ptr<fpdb::expression::gandiva::Expression> deserializeSubstrOperation(const json &jObj);
  shared_ptr<fpdb::expression::gandiva::Expression> deserializeCastOperation(const json &jObj);
  shared_ptr<fpdb::expression::gandiva::Expression> deserializeConcatOperation(const json &jObj);
  shared_ptr<::arrow::DataType> deserializeDataType(const json &jObj);
  shared_ptr<fpdb::expression::gandiva::Expression> deserializeExpression(const json &jObj);

  unordered_map<string, string> deserializeColumnRenames(const vector<json> &jArr);
  using HashJoinColumnNames = pair<vector<string>, vector<string>>;
  using HashJoinInputExprs = pair<vector<shared_ptr<fpdb::expression::gandiva::Expression>>,
                                  vector<shared_ptr<fpdb::expression::gandiva::Expression>>>;
  struct HashJoinConditionRes {
    HashJoinColumnNames joinColumnNames_;
    HashJoinInputExprs inputExprs_;
    HashJoinColumnNames inputExprNames_;
  };
  HashJoinConditionRes deserializeHashJoinCondition(const json &jObj, uint prePOpId,
                                                    int* leftInputExprId, int* rightInputExprId);
  void addProjectForJoinColumnRenames(shared_ptr<PrePhysicalOp> &op,
                                      const vector<shared_ptr<PrePhysicalOp>> &producers,
                                      const json &jObj);
  void addProjectForJoinInputExprs(const shared_ptr<PrePhysicalOp> &hashJoinPrePOp,
                                   const HashJoinConditionRes &joinCondRes);
  void addProjectForUnionInputRenames(shared_ptr<PrePhysicalOp> &op,
                                      const vector<shared_ptr<PrePhysicalOp>> &producers,
                                      const json &jObj);
  vector<SortKey> deserializeSortKeys(const json &jObj);

  shared_ptr<SortPrePOp> deserializeSort(const json &jObj);
  shared_ptr<LimitSortPrePOp> deserializeLimitSort(const json &jObj);
  shared_ptr<PrePhysicalOp> deserializeAggregateOrGroup(json &jObj);
  shared_ptr<PrePhysicalOp> deserializeProject(const json &jObj);
  shared_ptr<HashJoinPrePOp> deserializeHashJoin(const json &jObj);
  shared_ptr<NestedLoopJoinPrePOp> deserializeNestedLoopJoin(const json &jObj);
  shared_ptr<PrePhysicalOp> deserializeFilterOrFilterableScan(const json &jObj);
  shared_ptr<FilterableScanPrePOp> deserializeTableScan(const json &jObj);
  shared_ptr<UnionAllPrePOp> deserializeUnionAll(const json &jObj);

  string planJsonString_;
  shared_ptr<CatalogueEntry> catalogueEntry_;

  std::atomic<uint> pOpIdGenerator_;    // used to give each prePOp a unique id, in case there are different but identical
                                        // prePOps (e.g. two scans on the same table)
};


}


#endif //FPDB_FPDB_PLAN_INCLUDE_FPDB_PLAN_CALCITEPLANJSONDESERIALIZER_H
