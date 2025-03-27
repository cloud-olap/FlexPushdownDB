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

  /**
   * Set this op to be "pre-shuffle" in dist exec that shuffles data to nodes instead of threads
   * @param batchExchange
   * @param batchExchangeConsumers
   */
  void enableDistBatchExchange(const std::string &batchExchange,
                               const vector<string> &batchExchangeConsumers);

  /**
   * Add an additional set of consumers to fetch shuffle results, need to guarantee its size is same as "consumerVec_"
   * @param addConsumerVec
   */
  void produceAddiConsumerVec(const vector<shared_ptr<PhysicalOp>> &addiConsumerOps);

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
   * Shuffle one tupleSet
   */
  tl::expected<std::vector<std::shared_ptr<TupleSet>>, std::string>
  shuffle(const std::shared_ptr<TupleSet> &tupleSet);

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
  vector<string> consumerVec_;    // this always decides how many slots we shuffle to,
                                  // but may not be direct shuffle result receivers

  /**
   * Whether to enable batch exchange, if true,
   *  1. this op will be pre-shuffle in dist exec, i.e., shuffle to all nodes instead of all threads
   *  2. "consumerVec_" will be the receiver in different nodes
   *  3. shuffled data is always sent to the single "batchExchange_", in the form of "TupleSetBufferMessage"
   */
  bool isDistPreShuffle_ = false;
  std::string batchExchange_;

  /**
   * Sometimes the same shuffle results need to be sent to another additional set of consumers,
   * need to guarantee each set of additional consumers is the same size of "consumerVec_"
   */
  vector<vector<string>> addiConsumerVecs_;

  vector<std::optional<shared_ptr<TupleSet>>> buffers_;

// caf inspect
public:
  template <class Inspector>
  friend bool inspect(Inspector& f, ShufflePOp& op) {
    return inspect_base(f, op,
                        f.field("shuffleColumnNames", op.shuffleColumnNames_),
                        f.field("consumerVec", op.consumerVec_),
                        f.field("isDistPreShuffle", op.isDistPreShuffle_),
                        f.field("batchExchange", op.batchExchange_),
                        f.field("addiConsumerVecs", op.addiConsumerVecs_));
  }
};

}

#endif //FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_SHUFFLE_SHUFFLEPOP_H
