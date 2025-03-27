//
// Created by matt on 5/12/19.
//

#include <fpdb/executor/physical/PhysicalOp.h>
#include <fpdb/executor/physical/bloomfilter/BloomFilterBase.h>
#include <fpdb/executor/flight/FlightClients.h>
#include <fpdb/executor/message/NetworkMetricsMessage.h>
#include <fpdb/store/server/flight/GetBitmapTicket.hpp>
#include <fpdb/tuple/util/Util.h>
#include <spdlog/spdlog.h>
#include <cassert>               // for assert
#include <utility>               // for move

namespace fpdb::executor::physical {

PhysicalOp::PhysicalOp(std::string name,
                       POpType type,
                       std::vector<std::string> projectColumnNames,
                       int nodeId) :
  name_(std::move(name)),
  type_(type),
  projectColumnNames_(std::move(projectColumnNames)),
  nodeId_(nodeId),
  isSeparated_(false) {}

POpType PhysicalOp::getType() const {
  return type_;
}

std::string &PhysicalOp::name() {
  return name_;
}

const std::vector<std::string> &PhysicalOp::getProjectColumnNames() const {
  return projectColumnNames_;
}

int PhysicalOp::getNodeId() const {
  return nodeId_;
}

void PhysicalOp::setProducers(const std::set<std::string> &producers) {
  producers_ = producers;
}

void PhysicalOp::setConsumers(const std::set<std::string> &consumers) {
  consumers_ = consumers;
}

void PhysicalOp::produce(const std::shared_ptr<PhysicalOp> &op) {
  consumers_.emplace(op->name());
}

void PhysicalOp::consume(const std::shared_ptr<PhysicalOp> &op) {
  producers_.emplace(op->name());
}

void PhysicalOp::unProduce(const std::shared_ptr<PhysicalOp> &op) {
  consumers_.erase(op->name());
}

void PhysicalOp::unConsume(const std::shared_ptr<PhysicalOp> &op) {
  producers_.erase(op->name());
}

void PhysicalOp::reProduce(const std::string &oldOp, const std::string &newOp) {
  consumers_.erase(oldOp);
  consumers_.emplace(newOp);
}

void PhysicalOp::reConsume(const std::string &oldOp, const std::string &newOp) {
  producers_.erase(oldOp);
  producers_.emplace(newOp);
}

void PhysicalOp::clearProducers() {
  producers_.clear();
}

void PhysicalOp::clearConsumers() {
  consumers_.clear();
}

void PhysicalOp::clearConnections() {
  producers_.clear();
  consumers_.clear();
}

void PhysicalOp::addConsumerToBloomFilterInfo(const std::string &consumer,
                                              const std::string &bloomFilterCreatePOp,
                                              const std::vector<std::string> &columnNames) {
  consumerToBloomFilterInfo_[consumer] =
          std::make_shared<fpdb_store::FPDBStoreBloomFilterUseInfo>(bloomFilterCreatePOp, columnNames);
}

void PhysicalOp::setConsumerToBloomFilterInfo(const std::unordered_map<std::string,
        std::shared_ptr<fpdb_store::FPDBStoreBloomFilterUseInfo>> &consumerToBloomFilterInfo) {
  consumerToBloomFilterInfo_ = consumerToBloomFilterInfo;
}

std::set<std::string> PhysicalOp::consumers() {
  return consumers_;
}

std::set<std::string> PhysicalOp::producers() {
  return producers_;
}

const std::unordered_map<std::string, std::shared_ptr<fpdb_store::FPDBStoreBloomFilterUseInfo>>&
PhysicalOp::getConsumerToBloomFilterInfo() const {
  return consumerToBloomFilterInfo_;
}

std::shared_ptr<POpContext> PhysicalOp::ctx() {
  return opContext_;
}

void PhysicalOp::create(const std::shared_ptr<POpContext>& ctx) {
  assert (ctx);
  SPDLOG_DEBUG("Creating operator  |  name: '{}'", this->name_);
  opContext_ = ctx;
}

void PhysicalOp::setName(const std::string &Name) {
  name_ = Name;
}

void PhysicalOp::setProjectColumnNames(const std::vector<std::string> &projectColumnNames) {
  projectColumnNames_ = projectColumnNames;
}

void PhysicalOp::destroyActor() {
  opContext_->destroyActorHandles();
}

long PhysicalOp::getQueryId() const {
  return queryId_;
}

void PhysicalOp::setQueryId(long queryId) {
  queryId_ = queryId;
}

bool PhysicalOp::isSeparated() const {
  return isSeparated_;
}

void PhysicalOp::setSeparated(bool isSeparated) {
  isSeparated_ = isSeparated;
}

#if SHOW_DEBUG_METRICS == true
const metrics::PredTransMetrics::PTMetricsInfo &PhysicalOp::getPTMetricsInfo() const {
  return ptMetricsInfo_;
}

const metrics::PredTransCSMetrics::PTCSMetricsInfo &PhysicalOp::getPTCSMetricsInfo() const {
  return ptCSMetricsInfo_;
}

metrics::PredTransMetrics::PTPhaseType PhysicalOp::getPTPhaseType() const {
  return ptPhaseType_;
}

void PhysicalOp::setCollPredTransMetrics(const metrics::PredTransMetrics::PTMetricsInfo &ptMetricsInfo) {
  ptMetricsInfo_ = ptMetricsInfo;
}

void PhysicalOp::unsetCollPredTransMetrics() {
  ptMetricsInfo_.collPredTransMetrics_ = false;
}

void PhysicalOp::setPTPhaseType(metrics::PredTransMetrics::PTPhaseType ptPhaseType) {
  ptPhaseType_ = ptPhaseType;
}

void PhysicalOp::setCollPredTransCSMetrics(const metrics::PredTransCSMetrics::PTCSMetricsInfo &ptCSMetricsInfo) {
  ptCSMetricsInfo_ = ptCSMetricsInfo;
}
#endif

void PhysicalOp::sendPTCardMessage(const executor::cache::PredTransCardCache::PredTransCardInfo &ptCardInfo,
                                   int64_t numRows, std::optional<double> keyLen) {
  if (ptCardInfo.collect_) {
    executor::cache::PredTransCardCache::PredTransCardValue value(numRows, keyLen);
    auto ptCardMessage = std::make_shared<PredTransCardMessage>(ptCardInfo.key_, value, name_);
    ctx()->notifyRoot(ptCardMessage);
  }
}

void PhysicalOp::readRemoteBloomFilter(void* bloomFilter, const std::string &sender,
                                       const RemoteInfo &remoteInfo, bool remoteConsumerSpecific) {
  // fetch bitmap using Flight, record network time here
  auto start = std::chrono::steady_clock::now();

  auto client = flight::GlobalFlightClients.getFlightClient(remoteInfo.host_, remoteInfo.port_);
  auto ticketObj = store::server::flight::GetBitmapTicket::make(queryId_, sender);
  if (remoteConsumerSpecific) {
    ticketObj->set_consumer(name_);
  }
  auto expTicket = ticketObj->to_ticket(false);
  if (!expTicket.has_value()) {
    ctx()->notifyError(expTicket.error());
    return;
  }
  auto expReader = client->DoGet(*expTicket);
  if (!expReader.ok()) {
    ctx()->notifyError(expReader.status().message());
    return;
  }
  auto expRecordBatches = (*expReader)->ToRecordBatches();
  if (!expRecordBatches.ok()) {
    ctx()->notifyError(expRecordBatches.status().message());
    return;
  }

  auto finish = std::chrono::steady_clock::now();
  auto elapsedTime = std::chrono::duration_cast<std::chrono::nanoseconds>(finish - start).count();
  ctx()->operatorActor()->incrementNetworkTime(elapsedTime);

  // save bitmap into the bloom filter
  auto res = ((BloomFilterBase*)bloomFilter)->saveBitmapRecordBatches(*expRecordBatches);
  if (!res.has_value()) {
    ctx()->notifyError(res.error());
    return;
  }

  // metrics
#if SHOW_DEBUG_METRICS == true
  int64_t recordBatchesSize = 0;
  for (const auto &batch: *expRecordBatches) {
    recordBatchesSize += fpdb::tuple::util::Util::getSize(batch);
  }
  std::shared_ptr<Message> execMetricsMsg =
    std::make_shared<NetworkMetricsMessage>(metrics::NetworkMetrics(0, 0, recordBatchesSize), name_);
  ctx()->notifyRoot(execMetricsMsg);
#endif
}

} // namespace

