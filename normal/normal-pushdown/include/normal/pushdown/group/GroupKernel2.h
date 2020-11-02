//
// Created by matt on 20/10/20.
//

#ifndef NORMAL_NORMAL_PUSHDOWN_INCLUDE_NORMAL_PUSHDOWN_GROUP_GROUPKERNEL2_H
#define NORMAL_NORMAL_PUSHDOWN_INCLUDE_NORMAL_PUSHDOWN_GROUP_GROUPKERNEL2_H

#include <vector>
#include <string>
#include <memory>
#include <unordered_map>

#include <normal/tuple/TupleSet2.h>

#include <normal/pushdown/aggregate/AggregationFunction.h>
#include <normal/pushdown/group/GroupKey.h>
#include <normal/tuple/ArrayAppender.h>
#include "GroupKey2.h"

using namespace normal::pushdown::aggregate;
using namespace normal::tuple;
using namespace normal::expression;

namespace normal::pushdown::group {

using ArrayAppenderVector = std::vector<std::shared_ptr<ArrayAppender>>;

using GroupArrayAppenderVectorMap = std::unordered_map<std::shared_ptr<GroupKey2>,
													   ArrayAppenderVector,
													   GroupKey2PointerHash,
													   GroupKey2PointerPredicate>;

using GroupArrayVectorMap = std::unordered_map<std::shared_ptr<GroupKey2>,
											   ::arrow::ArrayVector,
											   GroupKey2PointerHash,
											   GroupKey2PointerPredicate>;

using GroupAggregationResultVectorMap = std::unordered_map<std::shared_ptr<GroupKey2>,
														   std::vector<std::shared_ptr<AggregationResult>>,
														   GroupKey2PointerHash,
														   GroupKey2PointerPredicate>;

class GroupKernel2 {

public:
  GroupKernel2(const std::vector<std::string>& groupColumnNames,
         const std::vector<std::string>& aggregateColumnNames,
			   std::vector<std::shared_ptr<AggregationFunction>> aggregateFunctions);

  /**
   * Groups the input tuple set and computes intermediate aggregates
   *
   * @param tupleSet
   * @return
   */
  [[nodiscard]] tl::expected<void, std::string> group(TupleSet2 &tupleSet);

  /**
   * Computes final aggregates and generates output tuple set
   *
   * @return
   */
  [[nodiscard]] tl::expected<std::shared_ptr<TupleSet2>, std::string> finalise();

private:
  std::vector<std::string> groupColumnNames_;
  std::vector<std::string> aggregateColumnNames_;
  std::vector<std::shared_ptr<AggregationFunction>> aggregateFunctions_;

  std::vector<int> groupColumnIndices_;
  std::vector<int> aggregateColumnIndices_;
  std::optional<std::shared_ptr<arrow::Schema>> inputSchema_;
  std::optional<std::shared_ptr<arrow::Schema>> outputSchema_;
  std::optional<std::shared_ptr<arrow::Schema>> aggregateSchema_;

  GroupArrayAppenderVectorMap groupArrayAppenderVectorMap_;
  GroupArrayVectorMap groupArrayVectorMap_;
  GroupAggregationResultVectorMap groupAggregationResultVectorMap_;

  // FIXME: this is a workaround because we cannot append a single scalar to appender directly
  GroupArrayVectorMap groupKeyBuffer_;

  /**
   * Caches input schema, indices to group columns, and makes output schema
   *
   * @param tupleSet
   * @return
   */
  [[nodiscard]] tl::expected<void, std::string> cache(const TupleSet2 &tupleSet);

  /**
   * Builds the output schema
   *
   * @return
   */
  tl::expected<std::shared_ptr<arrow::Schema>, std::string> makeOutputSchema();

  /**
   * Groups a single record batch
   *
   * @param recordBatch
   * @return
   */
  [[nodiscard]] tl::expected<GroupArrayVectorMap, std::string>
  groupRecordBatch(const ::arrow::RecordBatch &recordBatch);

  /**
   * Groups and computes intermediate aggregates for a single table
   *
   * @param recordBatch
   * @return
   */
  [[nodiscard]] tl::expected<void, std::string> groupTable(const ::arrow::Table &table);

  /**
   * Computes intermediate aggregates for a table
   *
   * @param table
   */
  void computeGroupAggregates();
};

}

#endif //NORMAL_NORMAL_PUSHDOWN_INCLUDE_NORMAL_PUSHDOWN_GROUP_GROUPKERNEL2_H
