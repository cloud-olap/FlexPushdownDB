//
// Created by Yifei Yang on 12/13/21.
//

#ifndef FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_NESTEDLOOPJOIN_RECORDBATCHNESTEDLOOPJOINER_H
#define FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_NESTEDLOOPJOIN_RECORDBATCHNESTEDLOOPJOINER_H

#include <fpdb/expression/gandiva/Expression.h>
#include <fpdb/expression/gandiva/Filter.h>
#include <fpdb/tuple/ArrayAppender.h>
#include <fpdb/tuple/TupleSet.h>

using namespace fpdb::tuple;
using namespace fpdb::expression::gandiva;
using namespace std;

namespace fpdb::executor::physical::join {

class RecordBatchNestedLoopJoiner {

public:
  RecordBatchNestedLoopJoiner(const optional<shared_ptr<expression::gandiva::Expression>> &predicate,
                              const shared_ptr<::arrow::Schema> &outputSchema,
                              const vector<shared_ptr<pair<bool, int>>> &neededColumnIndice);
  
  static shared_ptr<RecordBatchNestedLoopJoiner> make(const optional<shared_ptr<expression::gandiva::Expression>> &predicate,
                                                      const shared_ptr<::arrow::Schema> &outputSchema,
                                                      const vector<shared_ptr<pair<bool, int>>> &neededColumnIndice);

  tl::expected<void, string> join(const shared_ptr<::arrow::RecordBatch> &leftRecordBatch,
                                  const shared_ptr<::arrow::RecordBatch> &rightRecordBatch,
                                  int64_t leftRowOffset,
                                  int64_t rightRowOffset);

  tl::expected<shared_ptr<TupleSet>, string> toTupleSet();

  const unordered_set<int64_t> &getLeftRowMatchIndexes() const;
  const unordered_set<int64_t> &getRightRowMatchIndexes() const;

private:
  /**
   * Compute the cartesian product of two input record batch
   */
  tl::expected<shared_ptr<arrow::RecordBatch>, string>
  cartesian(const shared_ptr<::arrow::RecordBatch> &leftRecordBatch,
            const shared_ptr<::arrow::RecordBatch> &rightRecordBatch);

  /**
   * Filter the cartesian product computed above, and compute row match indexes of two input batches
   */
  tl::expected<arrow::ArrayVector, string> filter(const shared_ptr<arrow::RecordBatch> &recordBatch,
                                                  int64_t leftBatchNumRows,
                                                  int64_t leftRowOffset,
                                                  int64_t rightRowOffset);

  /**
   * Compute row match indexes of two input batches using the selection vector of filtering on cartesian product
   */
  pair<unordered_set<int64_t>, unordered_set<int64_t>>
  computeInputRowMatchIndexes(const shared_ptr<gandiva::SelectionVector> &selectionVector,
                              int64_t leftBatchNumRows,
                              int64_t leftRowOffset,
                              int64_t rightRowOffset) const;

  optional<shared_ptr<expression::gandiva::Expression>> predicate_;
  optional<std::shared_ptr<fpdb::expression::gandiva::Filter>> filter_;

  shared_ptr<::arrow::Schema> outputSchema_;
  shared_ptr<::arrow::Schema> leftOutputSchema_;
  shared_ptr<::arrow::Schema> rightOutputSchema_;
  vector<int> neededLeftColumnIndexes_;
  vector<int> neededRightColumnIndexes_;
  
  vector<::arrow::ArrayVector> joinedArrayVectors_;
  unordered_set<int64_t> leftRowMatchIndexes_;
  unordered_set<int64_t> rightRowMatchIndexes_;
  
};

}


#endif //FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_NESTEDLOOPJOIN_RECORDBATCHNESTEDLOOPJOINER_H
