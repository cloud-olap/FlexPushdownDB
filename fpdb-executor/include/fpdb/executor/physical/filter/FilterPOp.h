//
// Created by matt on 6/5/20.
//

#ifndef FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_FILTER_FILTERPOP_H
#define FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_FILTER_FILTERPOP_H

#include <fpdb/executor/physical/PhysicalOp.h>
#include <fpdb/executor/physical/fpdb-store/FPDBStoreFilterBitmapWrapper.h>
#include <fpdb/executor/message/TupleSetMessage.h>
#include <fpdb/executor/message/CompleteMessage.h>
#include <fpdb/cache/SegmentKey.h>
#include <fpdb/catalogue/Table.h>
#include <fpdb/expression/gandiva/Expression.h>
#include <fpdb/expression/gandiva/Filter.h>
#include <fpdb/tuple/TupleSet.h>
#include <memory>

using namespace fpdb::executor::message;
using namespace fpdb::executor::physical::fpdb_store;
using namespace fpdb::catalogue;

namespace fpdb::executor::physical::filter {

class FilterPOp : public PhysicalOp {

public:
  explicit FilterPOp(std::string name,
                  std::vector<std::string> projectColumnNames,
                  int nodeId,
                  std::shared_ptr<fpdb::expression::gandiva::Expression> predicate,
                  std::shared_ptr<Table> table = nullptr,
                  std::vector<std::shared_ptr<fpdb::cache::SegmentKey>> weightedSegmentKeys = {});
  FilterPOp() = default;
  FilterPOp(const FilterPOp&) = default;
  FilterPOp& operator=(const FilterPOp&) = default;

  void onReceive(const Envelope &Envelope) override;
  void clear() override;
  std::string getTypeString() const override;

  const std::shared_ptr<fpdb::expression::gandiva::Expression> &getPredicate() const;
  const std::optional<FPDBStoreFilterBitmapWrapper> &getBitmapWrapper() const;
  void setBitmapWrapper(const FPDBStoreFilterBitmapWrapper &bitmapWrapper);

  bool isBitmapPushdownEnabled();
  void enableBitmapPushdown(const std::string &fpdbStoreSuperPOp,
                            const std::string &mirrorOp,
                            bool isComputeSide,
                            const std::string &host,
                            int port);
  void setBitmap(const std::optional<std::vector<int64_t>> &bitmap);

  [[nodiscard]] size_t getFilterTimeNS() const;
  [[nodiscard]] size_t getFilterInputBytes() const;
  [[nodiscard]] size_t getFilterOutputBytes() const;

private:
  std::shared_ptr<fpdb::expression::gandiva::Expression> predicate_;

  std::optional<std::shared_ptr<fpdb::expression::gandiva::Filter>> filter_;

  /**
   * A buffer of received tuples not yet filtered
   */
  std::optional<std::shared_ptr<fpdb::tuple::TupleSet>> received_;

  /**
   * A buffer of filtered tuples not yet sent
   */
  std::optional<std::shared_ptr<fpdb::tuple::TupleSet>> filtered_;

  void onStart();
  void onTupleSet(const TupleSetMessage& Message);
  void onTupleSetRegular();
  void onTupleSetBitmapPushdown();
  void onComplete(const CompleteMessage& Message);
  void onCompleteRegular();
  void onCompleteBitmapPushdown();

  void bufferReceived(const std::shared_ptr<fpdb::tuple::TupleSet>& tupleSet);
  void bufferFiltered(const std::shared_ptr<fpdb::tuple::TupleSet>& tupleSet);
  void bufferBitMap(const std::shared_ptr<::gandiva::SelectionVector> &selectionVector,
                    int64_t rowOffset,
                    int64_t inputNumRows);
  std::shared_ptr<::gandiva::SelectionVector> makeSelectionVector(int64_t startRowOffset, int64_t numRows);

  void buildFilter();
  void filterTuplesRegular();
  void filterTuplesSaveBitMap();
  void filterTuplesUsingBitmap();

  void sendTuples();
  void sendEmpty();
  void sendSegmentWeight();
  void sendBitmapToRoot();
  void putBitmapToFPDBStore();
  void getBitmapFromFPDBStore();

  void checkApplicability(const std::shared_ptr<fpdb::tuple::TupleSet>& tupleSet);
  bool isComputeSide();
  bool isBitmapSet();

  int64_t totalNumRows_ = 0;
  int64_t filteredNumRows_ = 0;
  size_t filterTimeNS_ = 0;
  size_t inputBytesFiltered_ = 0;
  size_t outputBytesFiltered_ = 0;

  /**
   * Whether all predicate columns are covered in the schema of received tuples
   */
  std::optional<bool> isApplicable_;

  /**
   * Used to compute filter weight, set to nullptr and {} if its producer is not table scan
   */
  std::shared_ptr<Table> table_;
  std::vector<std::shared_ptr<fpdb::cache::SegmentKey>> weightedSegmentKeys_;

  /**
   * Used for bitmap pushdown, i.e. to save filter result as bitmap
   * It has value iff bitmap pushdown is enabled
   */
  std::optional<FPDBStoreFilterBitmapWrapper> bitmapWrapper_;

// caf inspect
public:
  template <class Inspector>
  friend bool inspect(Inspector& f, FilterPOp& op) {
    return f.object(op).fields(f.field("name", op.name_),
                               f.field("type", op.type_),
                               f.field("projectColumnNames", op.projectColumnNames_),
                               f.field("nodeId", op.nodeId_),
                               f.field("queryId", op.queryId_),
                               f.field("opContext", op.opContext_),
                               f.field("producers", op.producers_),
                               f.field("consumers", op.consumers_),
                               f.field("consumerToBloomFilterInfo", op.consumerToBloomFilterInfo_),
                               f.field("isSeparated", op.isSeparated_),
                               f.field("predicate", op.predicate_),
                               f.field("table", op.table_),
                               f.field("weightedSegmentKeys", op.weightedSegmentKeys_),
                               f.field("bitmapWrapper", op.bitmapWrapper_));
  }
};

inline bool recordSpeeds = false;
inline size_t totalBytesFiltered_ = 0;
}

#endif //FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_FILTER_FILTERPOP_H
