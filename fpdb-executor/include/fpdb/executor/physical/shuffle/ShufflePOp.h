//
// Created by matt on 17/6/20.
//

#ifndef FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_SHUFFLE_SHUFFLEPOP_H
#define FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_SHUFFLE_SHUFFLEPOP_H

#include <fpdb/executor/physical/PhysicalOp.h>
#include <fpdb/executor/message/CompleteMessage.h>
#include <fpdb/executor/message/TupleSetMessage.h>
#include <fpdb/tuple/TupleSet.h>

using namespace fpdb::executor::message;
using namespace fpdb::tuple;
using namespace std;

namespace fpdb::executor::physical::shuffle {

/**
 * A firts cut of a shuffle operator, shuffles on a single key only
 */
class ShufflePOp : public PhysicalOp {

public:
  ShufflePOp(string name,
             vector<string> projectColumnNames,
             int nodeId,
             vector<string> shuffleColumnNames);
  ShufflePOp(string name,
             vector<string> projectColumnNames,
             int nodeId,
             vector<string> shuffleColumnNames,
             vector<string> consumerVec);
  ShufflePOp() = default;
  ShufflePOp(const ShufflePOp&) = default;
  ShufflePOp& operator=(const ShufflePOp&) = default;

  /**
   * Operators message handler
   * @param msg
   */
  void onReceive(const Envelope &msg) override;
  void clear() override;
  std::string getTypeString() const override;

  const std::vector<std::string> &getShuffleColumnNames() const;
  const std::vector<std::string> &getConsumerVec() const;
  void setConsumerVec(const std::vector<std::string> &consumerVec);

  /**
   * This only adds op to consumerVec_
   * @param op
   */
  void addToConsumerVec(const std::shared_ptr<PhysicalOp> &op);

  /**
   * Clear consumerVec_
   */
  void clearConsumerVec();

  /**
   * Set the producer operator, i.e. add operator_ to both consumers_(base class) and consumerVec_
   * @param operator_
   */
  void produce(const shared_ptr<PhysicalOp> &operator_) override;

private:
  /**
   * Start message handler
   */
  void onStart();

  /**
   * Completion message handler
   */
  void onComplete(const CompleteMessage &);

  /**
   * Tuples message handler
   * @param message
   */
  void onTupleSet(const TupleSetMessage &message);

  /**
   * Adds the tuple set to the outbound buffer for the given slot
   * @param tupleSet
   * @param partitionIndex
   * @return
   */
  [[nodiscard]] tl::expected<void, string> buffer(const shared_ptr<TupleSet>& tupleSet, int partitionIndex);

  /**
   * Sends the buffered tupleset if its big enough or force is true
   * @param partitionIndex
   * @param force
   * @return
   */
  [[nodiscard]] tl::expected<void, string> send(int partitionIndex, bool force);

  vector<string> shuffleColumnNames_;
  vector<string> consumerVec_;
  vector<std::optional<shared_ptr<TupleSet>>> buffers_;

// caf inspect
public:
  template <class Inspector>
  friend bool inspect(Inspector& f, ShufflePOp& op) {
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
                               f.field("shuffleColumnNames", op.shuffleColumnNames_),
                               f.field("consumerVec", op.consumerVec_));
  }
};

}

#endif //FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_SHUFFLE_SHUFFLEPOP_H
