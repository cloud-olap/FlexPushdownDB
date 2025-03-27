//
// Created by Yifei Yang on 3/16/22.
//

#ifndef FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_BLOOMFILTER_BLOOMFILTERUSEPOP_H
#define FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_BLOOMFILTER_BLOOMFILTERUSEPOP_H

#include <fpdb/executor/physical/PhysicalOp.h>
#include <fpdb/executor/message/BloomFilterMessage.h>
#include <fpdb/executor/metrics/PredTransMetrics.h>
#include <fpdb/tuple/RecordBatchHasher.h>
#include <mutex>

namespace fpdb::executor::physical::bloomfilter {

class BloomFilterUsePOp: public PhysicalOp {
  
public:
  explicit BloomFilterUsePOp(const std::string &name,
                             const std::vector<std::string> &projectColumnNames,
                             int nodeId,
                             const std::vector<std::string> &bloomFilterColumnNames,
                             int numExpBloomFilters = 1);

  BloomFilterUsePOp() = default;
  BloomFilterUsePOp(const BloomFilterUsePOp&) = default;
  BloomFilterUsePOp& operator=(const BloomFilterUsePOp&) = default;
  ~BloomFilterUsePOp() = default;

  void onReceive(const Envelope &envelope) override;
  void clear() override;
  std::string getTypeString() const override;

  const std::vector<std::string> &getBloomFilterColumnNames() const;
  const std::shared_ptr<BloomFilterBase> &getStandAloneBloomFilter() const;
  void setStandAloneBloomFilter(const std::shared_ptr<BloomFilterBase> &bloomFilter);
  bool receivedStandAloneBloomFilter() const;
  void clearProducersExceptBloomFilterCreate();

#if SHOW_DEBUG_METRICS == true
  int64_t getNumRowsInput() const;
#endif

  void recordPredTransCard(const executor::cache::PredTransCardCache::PredTransCardKey &key);

private:
  void onStart();
  void onComplete(const CompleteMessage &);

  void onTupleSet(const TupleSetMessage &msg);
  void onTupleSetStandAlone(const std::shared_ptr<TupleSet> &tupleSet);
  void onTupleSetDist(const std::shared_ptr<TupleSet> &tupleSet);

  void onBloomFilter(const BloomFilterMessage &msg);
  void onBloomFilterStandAlone(const std::shared_ptr<BloomFilterBase> &bloomFilter);
  void onBloomFilterDist(const std::shared_ptr<BloomFilterBase> &bloomFilter);

  // used by standalone exec
  tl::expected<void, std::string> filterAndSend();

  // used by dist exec
  tl::expected<void, std::string> makeHasher(const std::shared_ptr<arrow::Schema> &schema);
  // hash the bloom filter column for the table
  tl::expected<uint32_t*, std::string> hash32(const std::shared_ptr<TupleSet> &tupleSet);
  tl::expected<uint64_t*, std::string> hash64(const std::shared_ptr<TupleSet> &tupleSet);
  // get bitmap as filter output, filter one incoming table (last one in distState_.tupleSets_) on all BFs
  tl::expected<void, std::string> filterOneTupleSet(uint tupleSetIdx);
  // get bitmap as filter output, filter all tables on one incoming BF (last one in distState_.bloomFilterVec_)
  tl::expected<void, std::string> filterOneBloomFilter(uint bloomFilterIdx);
  // called by above
  tl::expected<uint8_t*, std::string> filter(uint tupleSetIdx, uint bloomFilterIdx);
  // apply bitmap to input tables and send
  void debug(int idx);
  tl::expected<void, std::string> projectAndSend();
  uint8_t* bitvecOr(std::vector<uint8_t*> &bitVecs, int len);

  // input parameters
  std::vector<std::string> bloomFilterColumnNames_;
  int numExpBloomFilters_;   // how many bloom filters this op should receive
                             // 1 on standalone, >1 on distributed

  // runtime state
  std::shared_ptr<std::vector<int>> columnIndices_;    // used by vanilla bloom filter
  std::shared_ptr<RecordBatchHasher> hasher_;          // used by arrow blocked bloom filter

  // used by single-node exec
  struct StandAloneState {
    std::shared_ptr<TupleSet> tupleSet_;
    std::shared_ptr<BloomFilterBase> bloomFilter_;

    void clear() {
      tupleSet_.reset();
      bloomFilter_.reset();
    }
  } standAloneState_;

  // used by distributed exec
  struct DistState {
    std::vector<std::shared_ptr<TupleSet>> tupleSets_;
    std::vector<uint32_t*> hashesVec32_;         // save to reuse on multiple bfs, per table
    std::vector<uint64_t*> hashesVec64_;         // save to reuse on multiple bfs, per table
                                                 // a table computes either or both 32 and 64-bit hashes
    std::vector<std::shared_ptr<BloomFilterBase>> bloomFilterVec_;
    bool bloomFilterValid_ = true;               // one invalid bf makes no filtering on all tables
    std::vector<int64_t> bitvecLen_;             // length of output bitvectors below, never used for empty table
    std::vector<std::vector<uint8_t*>> outs_;    // store output as bitvector for all tupleSets for all bloom filters
                                                 // nullptr denotes no filtering (e.g. BF is invalid or table is empty)
                                                 // 1st dim: tupleSet, 2nd dim: bloom filter
    void clear() {
      for (const auto &hashes: hashesVec32_) {
        free(hashes);
      }
      for (const auto &hashes: hashesVec64_) {
        free(hashes);
      }
      for (const auto &outVec: outs_) {
        for (const auto &out: outVec) {
          free(out);
        }
      }
      tupleSets_.clear();
      hashesVec32_.clear();
      hashesVec64_.clear();
      bloomFilterVec_.clear();
      bitvecLen_.clear();
      outs_.clear();
    }
  } distState_;

#if SHOW_DEBUG_METRICS == true
  int64_t numRowsInput_ = 0;
#endif

  // to record runtime cardinalities
  executor::cache::PredTransCardCache::PredTransCardInfo ptCardInfo_;
  int64_t numRowsOutput_ = 0;

// caf inspect
public:
  template <class Inspector>
  friend bool inspect(Inspector& f, BloomFilterUsePOp& op) {
    return inspect_base(f, op,
                        f.field("bloomFilterColumnNames", op.bloomFilterColumnNames_),
                        f.field("numExpBloomFilters", op.numExpBloomFilters_),
                        f.field("ptCardInfo", op.ptCardInfo_));
  }
};

}


#endif //FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_BLOOMFILTER_BLOOMFILTERUSEPOP_H
