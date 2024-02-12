//
// Created by Yifei Yang on 2/21/22.
//

#ifndef FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_FPDB_STORE_FPDBSTORESUPERPOP_H
#define FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_FPDB_STORE_FPDBSTORESUPERPOP_H

#include <fpdb/executor/physical/PhysicalPlan.h>
#include <tl/expected.hpp>
#include <thread>
#include <mutex>
#include <condition_variable>

namespace fpdb::executor::physical::fpdb_store {

/**
 * This is used when FPDBStoreSuperPOp is detached, e.g. when filter bitmap pushdown is enabled,
 * otherwise too many detached FPDBStoreSuperPOp will cause performance degradation.
 */
inline std::mutex FPDBStoreSuperPOpDetachMutex;
inline std::unordered_map<std::string, std::shared_ptr<std::condition_variable_any>> FPDBStoreSuperPOpDetachCvs;
inline int numFPDBStoreSuperPOpDetachSlots = std::thread::hardware_concurrency();
// if filter bitmap cannot be generated at compute side, we shouldn't use this blocking mechanism
inline std::unordered_set<std::string> nonRestrictFilters;

/**
 * Denote a sub-plan to be pushed to store, consists a group of physical operators (e.g. scan->filter->aggregate)
 */
class FPDBStoreSuperPOp : public PhysicalOp {

public:
  FPDBStoreSuperPOp(const std::string &name,
                    const std::vector<std::string> &projectColumnNames,
                    int nodeId,
                    const std::shared_ptr<PhysicalPlan> &subPlan,
                    int parallelDegree,
                    const std::string &host,
                    int fileServicePort,
                    int flightPort);
  FPDBStoreSuperPOp() = default;
  FPDBStoreSuperPOp(const FPDBStoreSuperPOp&) = default;
  FPDBStoreSuperPOp& operator=(const FPDBStoreSuperPOp&) = default;
  ~FPDBStoreSuperPOp() = default;

  void onReceive(const Envelope &envelope) override;
  void clear() override;
  std::string getTypeString() const override;
  void produce(const std::shared_ptr<PhysicalOp> &op) override;

  const std::shared_ptr<PhysicalPlan> &getSubPlan() const;
  const std::string &getHost() const;
  int getFileServicePort() const;
  int getFlightPort() const;

  void setWaitForScanMessage(bool waitForScanMessage);
  void setReceiveByOthers(bool receiveByOthers);
  void setShufflePOp(const std::shared_ptr<PhysicalOp> &op);
  void addFPDBStoreBloomFilterProducer(const std::shared_ptr<PhysicalOp> &fpdbStoreBloomFilterProducer);
  void setForwardConsumers(const std::vector<std::shared_ptr<PhysicalOp>> &consumers);
  void resetForwardConsumers();
  void setGetAdaptPushdownMetrics(bool getAdaptPushdownMetrics);

  // used in filter bitmap pushdown
  static void unBlockNonRestrictFilters(const std::string &filterPOpName, const std::string &fpdbStoreSuperPOpName);

private:
  // used in filter bitmap pushdown
  void processDetachIn();
  void processDetachOut();

  void onStart();
  void onCacheLoadResponse(const ScanMessage &msg);
  void onBloomFilter(const BloomFilterMessage &);
  void onComplete(const CompleteMessage &);

  bool readyToProcess();
  void processAtStore();
  void processEmpty();
  void processAsPullup();   // for adaptive pushdown
  void onErrorDuringProcess(const std::string &error);    // handle errors from processAtStore()
  tl::expected<std::string, std::string> serialize(bool pretty);

  std::shared_ptr<PhysicalPlan> subPlan_;
  int parallelDegree_;
  std::string host_;
  int fileServicePort_;     // for adaptive pushdown
  int flightPort_;

  // if waiting for scan message before sending request to store
  bool waitForScanMessage_ = false;

  // set when the result of pushdown is received by consumers instead of here
  bool receiveByOthers_ = false;

  // set when pushing shuffle to store
  std::optional<std::string> shufflePOpName_ = std::nullopt;

  // set when pushing filter bitmap to store
  std::unordered_set<std::string> filterPOpNames_;

  // set when pushing bloom filter to store
  int numBloomFiltersExpected_ = 0;
  int numBloomFiltersReceived_ = 0;

  // for metrics of adaptive pushdown
  bool getAdaptPushdownMetrics_ = false;

// caf inspect
public:
  template <class Inspector>
  friend bool inspect(Inspector& f, FPDBStoreSuperPOp& op) {
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
                               f.field("subPlan", op.subPlan_),
                               f.field("parallelDegree", op.parallelDegree_),
                               f.field("host", op.host_),
                               f.field("fileServicePort", op.fileServicePort_),
                               f.field("flightPort", op.flightPort_),
                               f.field("waitForScanMessage", op.waitForScanMessage_),
                               f.field("receiveByOthers", op.receiveByOthers_),
                               f.field("shufflePOpName", op.shufflePOpName_),
                               f.field("numBloomFiltersExpected", op.numBloomFiltersExpected_),
                               f.field("numBloomFiltersReceived", op.numBloomFiltersReceived_),
                               f.field("getAdaptPushdownMetrics", op.getAdaptPushdownMetrics_));
  }

};

}


#endif //FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_FPDB_STORE_FPDBSTORESUPERPOP_H
