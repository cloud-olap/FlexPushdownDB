//
// Created by Yifei Yang on 11/23/21.
//

#include <fpdb/executor/Execution.h>
#include <fpdb/executor/Executor.h>
#include <fpdb/executor/Globals.h>
#include <fpdb/executor/Progress.h>
#include <fpdb/executor/physical/POpContext.h>
#include <fpdb/executor/physical/POpDirectoryEntry.h>
#include <fpdb/executor/physical/POpConnection.h>
#include <fpdb/executor/physical/POpRelationshipType.h>
#include <fpdb/executor/physical/Globals.h>
#include <fpdb/executor/physical/filter/FilterPOp.h>
#include <fpdb/executor/caf-serialization/CAFPOpSerializer.h>
#include <fpdb/executor/message/NetworkMetricsMessage.h>
#include <fpdb/executor/message/DiskMetricsMessage.h>
#include <fpdb/executor/message/PredTransMetricsMessage.h>
#include <fpdb/executor/message/PredTransCSMetricsMessage.h>
#include <fpdb/executor/message/HashJoinMetricsMessage.h>
#include <fpdb/executor/metrics/PredTransMetrics.h>
#include <fpdb/util/Util.h>
#include <fpdb/executor/flight/FlightClients.h>
#include <fpdb/store/server/flight/Util.hpp>
#include <fpdb/store/server/flight/adaptive/SetNumReqToTailCmd.hpp>
#include <caf/io/all.hpp>
#include <boost/asio/thread_pool.hpp>
#include <boost/asio/post.hpp>
#include <boost/bind.hpp>
#include <graphviz/gvc.h>

namespace fpdb::executor {

Execution::Execution(long queryId, 
                     const shared_ptr<::caf::actor_system> &actorSystem,
                     const vector<::caf::node_id> &nodes,
                     const ::caf::actor &localSegmentCacheActor,
                     const vector<::caf::actor> &remoteSegmentCacheActors,
                     const shared_ptr<PhysicalPlan> &physicalPlan,
                     bool isDistributed,
                     void* executor) :
  queryId_(queryId),
  actorSystem_(actorSystem),
  nodes_(nodes),
  localSegmentCacheActor_(localSegmentCacheActor),
  remoteSegmentCacheActors_(remoteSegmentCacheActors),
  physicalPlan_(physicalPlan),
  isDistributed_(isDistributed),
  executor_(executor) {
  rootActor_ = make_shared<::caf::scoped_actor>(*actorSystem_);
}

Execution::~Execution() {
  close();
}

void Execution::enableAdaptExec(const shared_ptr<AdaptPhysicalPlan> &adaptPhysicalPlan) {
  adaptSt_.isAdapt_ = true;
  adaptSt_.adaptPhysicalPlan_ = adaptPhysicalPlan;
  physicalPlan_ = adaptPhysicalPlan;
}

void Execution::execute() {
  preExecute();
  boot();
  start();
  join();
  if (adaptSt_.isAdapt_) {
    clear(true);
  }
}

shared_ptr<TupleSet> Execution::getQueryResult() const {
  return collateOp_->tuples();
}

void Execution::preExecute() {
  // Traverse the ops for some setups
  for (const auto &opIt: physicalPlan_->getPhysicalOps()) {
    auto op = opIt.second;
    // set query id
    op->setQueryId(queryId_);
    // add op to op directory
    auto result = opDirectory_.insert(POpDirectoryEntry(op, nullptr, false));
    if (!result.has_value()) {
      throw runtime_error(result.error());
    }
  }
  // Correct num ops to complete for adaptive exec
  if (adaptSt_.isAdapt_) {
    opDirectory_.addNumOperatorsToComplete(adaptSt_.adaptPhysicalPlan_->consumeAllSink() ?
                                                opDirectory_.numOperatorsLeftOver() :
                                                adaptSt_.adaptPhysicalPlan_->numSinkToComplete()
                                           -
                                           adaptSt_.adaptPhysicalPlan_->numSinkToProduce());
  }

  // If using adapt_pushdown_manager v3 and enable pushback tail req double-exec,
  // notify the storage num reqs to tail
  if (ENABLE_ADAPTIVE_PUSHDOWN &&
      store::server::flight::AdaptPushdownManagerV == store::server::flight::AdaptPushdownManagerVersion::V3 &&
      store::server::flight::EnablePushbackTailReqDoubleExec) {
    // collect num reqs
    std::optional<int> port = std::nullopt;
    std::unordered_map<std::string, int64_t> hostToNumReqs;
    for (const auto &opIt: physicalPlan_->getPhysicalOps()) {
      auto op = opIt.second;
      if (op->getType() == POpType::FPDB_STORE_SUPER) {
        auto typedOp = std::static_pointer_cast<FPDBStoreSuperPOp>(op);
        ++hostToNumReqs[typedOp->getHost()];
        if (!port.has_value()) {
          port = typedOp->getFlightPort();
        }
      }
    }
    // send requests
    if (port.has_value()) {
      for (const auto &it: hostToNumReqs) {
        auto client = flight::GlobalFlightClients.getFlightClient(it.first, *port);
        auto cmdObj = store::server::flight::SetNumReqToTailCmd::make(it.second);
        auto expCmd = cmdObj->serialize(false);
        if (!expCmd.has_value()) {
          throw std::runtime_error(expCmd.error());
        }
        auto descriptor = ::arrow::flight::FlightDescriptor::Command(*expCmd);
        auto doPutRes = client->DoPut(descriptor, nullptr);
        if (!doPutRes.ok()) {
          throw std::runtime_error(doPutRes.status().message());
        }
        auto status = (*doPutRes).writer->Close();
        if (!status.ok()) {
          throw std::runtime_error(status.message());
        }
      }
    }
  }
}

void Execution::boot() {
  // Tell segment cache actor that new query comes
  (*rootActor_)->anon_send(localSegmentCacheActor_, NewQueryAtom_v);
  for (const auto &remoteSegmentCacheActor: remoteSegmentCacheActors_) {
    (*rootActor_)->anon_send(remoteSegmentCacheActor, NewQueryAtom_v);
  }

  // Spawn actors locally/remotely according to nodeId assigned in parallel.
  // Here we use thread pool inside of actor because it's more complex to implement "join()" using actors, and actor's
  // message passing incurs higher overhead.
  auto thPool = PrepareExecutionInParallel ?
                std::make_unique<boost::asio::thread_pool>(std::thread::hardware_concurrency()) : nullptr;
  for (auto &element: opDirectory_) {
    auto &opEntry = element.second;
    // in adaptive exec, there may be some actors already spawned
    if (opEntry.getActorHandle() == nullptr) {
      thPool ? boost::asio::post(*thPool, boost::bind(&Execution::spawn, this, std::ref(opEntry))) : spawn(opEntry);
    }
  }

  // Wait if using thread pool
  if (thPool) {
    thPool->join();
  }
}

void Execution::start() {
  startTime_ = chrono::steady_clock::now();

  // Mark all the operators as incomplete
  opDirectory_.setIncomplete();

  // Parallel start, same reason as done in "boot()"
  auto thPool = PrepareExecutionInParallel ?
                std::make_unique<boost::asio::thread_pool>(std::thread::hardware_concurrency()) : nullptr;

  // Create a set of sender actors for better performance
  int numThreads = std::thread::hardware_concurrency();
  auto senders = PrepareExecutionInParallel ?
          vector<shared_ptr<::caf::scoped_actor>>(numThreads, make_shared<::caf::scoped_actor>(*actorSystem_)) :
          vector<shared_ptr<::caf::scoped_actor>>();

  // Connect the actors
  int i = 0;
  for (const auto &element: opDirectory_) {
    auto &entry = element.second;
    thPool ? boost::asio::post(*thPool, boost::bind(&Execution::sendConnect, this,
                                                    std::ref(entry), std::ref(*senders[i++ % numThreads])))
           : sendConnect(entry, *rootActor_);
  }

  // Wait if using thread pool
  if (thPool) {
    thPool->join();
    thPool.reset();
  }

  // Start the actors, need to restart a thread pool if using
  thPool = PrepareExecutionInParallel ?
           std::make_unique<boost::asio::thread_pool>(std::thread::hardware_concurrency()) : nullptr;
  i = 0;
  for (const auto &element: opDirectory_) {
    auto &entry = element.second;
    thPool ? boost::asio::post(*thPool, boost::bind(&Execution::sendStart, this,
                                                    std::ref(entry), std::ref(*senders[i++ % numThreads])))
           : sendStart(entry, *rootActor_);
  }

  // Wait if using thread pool
  if (thPool) {
    thPool->join();
  }
}

void Execution::join() {
  SPDLOG_DEBUG("Waiting for all operators to complete");

  auto handle_err = [&](const ::caf::error &err) {
    throw runtime_error(to_string(err));
  };

  // progress bar
  bool showProgress = metrics::SHOW_PROGRESS_BAR && !adaptSt_.isAdapt_;
  auto progress = showProgress ? std::make_shared<Progress>(physicalPlan_.get()) : nullptr;
  if (showProgress) {
    progress->display();
  }

  bool allComplete = false;
  (*rootActor_)->receive_while([&] { return !allComplete; })(
          [&](const Envelope &e) {
            const auto &msg = e.message();
            SPDLOG_DEBUG("Query root actor received message  |  query: '{}', messageKind: '{}', from: '{}'",
                         queryId_, msg.getTypeString(), msg.sender());

            auto errAct = [&](const std::string &errMsg) {
              allComplete = true;
              throw runtime_error(errMsg);
            };

            switch (msg.type()) {
              case MessageType::COMPLETE: {
                this->opDirectory_.setComplete(msg.sender())
                        .map_error(errAct);
                allComplete = this->opDirectory_.allComplete();
                if (showProgress) {
                  auto expOp = physicalPlan_->getPhysicalOp(msg.sender());
                  if (!expOp.has_value()) {
                    errAct(expOp.error());
                  }
                  progress->advance((*expOp)->getType(), false);
                }
                break;
              }

#if SHOW_DEBUG_METRICS == true
              case MessageType::NETWORK_METRICS: {
                auto networkMetricsMsg = ((NetworkMetricsMessage &) msg);
                debugMetrics_.add(networkMetricsMsg.getNetworkMetrics());
                int64_t bytesInterCompute = networkMetricsMsg.getNetworkMetrics().getBytesInterCompute();
                if (bytesInterCompute > 0) {
                  auto expOpEntry = opDirectory_.get(networkMetricsMsg.sender());
                  if (expOpEntry.has_value()) {
                    switch((*expOpEntry).getDef()->getPTPhaseType()) {
                      case metrics::PredTransMetrics::PRED_TRANS_PHASE: {
                        totalPredTransInterComputeBytes_ += bytesInterCompute;
                        break;
                      }
                      case metrics::PredTransMetrics::JOIN_PHASE: {
                        totalJoinInterComputeBytes_ += bytesInterCompute;
                        break;
                      }
                      default: {
                        break;
                      }
                    }
                  }
                }
                break;
              }

              case MessageType::DISK_METRICS: {
                auto diskMetricsMsg = ((DiskMetricsMessage &) msg);
                debugMetrics_.add(diskMetricsMsg.getDiskMetrics());
                break;
              }

              case MessageType::PRED_TRANS_METRICS: {
                auto ptMetricsMsg = ((PredTransMetricsMessage &) msg);
                debugMetrics_.add(ptMetricsMsg.getPTMetrics());
                break;
              }

              case MessageType::PRED_TRANS_CS_METRICS: {
                auto ptCSMetricsMsg = ((PredTransCSMetricsMessage &) msg);
                debugMetrics_.add(ptCSMetricsMsg.getPTCSMetrics());
                break;
              }

              case MessageType::HASH_JOIN_METRICS: {
                auto hjMetricsMsg = ((HashJoinMetricsMessage &) msg);
                debugMetrics_.add(hjMetricsMsg.getHashJoinMetrics());
                break;
              }

              case MessageType::PUSHDOWN_FALL_BACK: {
                debugMetrics_.incPushdownFallBack();
                break;
              }
#endif

              case MessageType::PRED_TRANS_CARD: {
                auto ptCardMsg = ((PredTransCardMessage &) msg);
                const auto &key = ptCardMsg.getKey();
                const auto &value = ptCardMsg.getValue();
                ((Executor*) executor_)->cardCache_.produce(key, value);
                break;
              }

              case MessageType::OUTPUT_CARD: {
                auto outputCardMsg = ((OutputCardMessage &) msg);
                const auto &key = outputCardMsg.getKey();
                int64_t card = outputCardMsg.getCard();
                ((Executor*) executor_)->cardCache_.produce(key, card);
                break;
              }

              case MessageType::ERROR: {
                errAct(fmt::format("ERROR: {}, from {}", ((ErrorMessage &) msg).getContent(), msg.sender()));
              }
              default: {
                errAct(fmt::format("Invalid message type sent to the root actor: {}, from {}", msg.getTypeString(), msg.sender()));
              }
            }

          },
          handle_err);

  stopTime_ = chrono::steady_clock::now();

  // progress bar
  if (showProgress) {
    progress->wait();
  }
}

void Execution::clear(bool forAdapt) {
  if (rootActor_ == nullptr) {
    throw runtime_error("Root actor not set on clear.");
  }
  for (auto it = opDirectory_.cbegin(); it != opDirectory_.cend(); ) {
    // delete completed entries, in static exec this will delete all entries;
    // in adaptive exec some entries are not complete which will be completed in later stages
    if (it->second.isComplete()) {
      (*rootActor_)->send_exit(it->second.getActorHandle(), ::caf::exit_reason::user_shutdown);
      it = opDirectory_.erase(it);
    } else {
      // in adaptive exec, clear consumers for preserved "sinkToConsume_"
      if (adaptSt_.isAdapt_) {
        bool preserve;
        if (adaptSt_.adaptPhysicalPlan_->isSinkToConsume(it->first, &preserve) && preserve) {
          it->second.getDef()->clearConsumers();
        }
      }
      ++it;
    }
  }
  if (forAdapt) {
    // correct num ops to complete for next stage
    opDirectory_.resetNumOps();
  } else {
    // close execution
    rootActor_.reset();
    collateOp_.reset();
  }
}

void Execution::close() {
  clear(false);
}

void Execution::spawn(POpDirectoryEntry &opEntry) {
  // create op context
  auto &op = opEntry.getDef();
  const auto &segmentCacheActor = isDistributed_ ?
                                  remoteSegmentCacheActors_.empty() ? nullptr : remoteSegmentCacheActors_[op->getNodeId()]:
                                  localSegmentCacheActor_;
  auto ctx = make_shared<POpContext>(*rootActor_, segmentCacheActor);
  op->create(ctx);

  // spawn, make collate at the coordinator
  if (op->getType() == POpType::COLLATE) {
    collateOp_ = static_pointer_cast<physical::collate::CollatePOp>(op);
    opEntry.setActorHandle(localSpawn(op));
  } else {
    if (isDistributed_) {
      opEntry.setActorHandle(remoteSpawn(op, op->getNodeId()));
    } else {
      opEntry.setActorHandle(localSpawn(op));
    }
  }
}

::caf::actor Execution::localSpawn(const shared_ptr<PhysicalOp> &op) {
  if (useDetached(op)) {
    auto actorHandle = actorSystem_->spawn<POpActor, detached>(op);
    if (!actorHandle)
      throw runtime_error(fmt::format("Failed to spawn operator actor '{}'", op->name()));
    return ::caf::actor_cast<::caf::actor>(actorHandle);
  } else {
    auto actorHandle = actorSystem_->spawn<POpActor>(op);
    if (!actorHandle)
      throw runtime_error(fmt::format("Failed to spawn operator actor '{}'", op->name()));
    return ::caf::actor_cast<::caf::actor>(actorHandle);
  }
}

::caf::actor Execution::remoteSpawn(const shared_ptr<PhysicalOp> &op, int nodeId) {
  auto remoteSpawnTout = std::chrono::seconds(10);
  auto args = make_message(op);
  auto actorTypeName = useDetached(op) ? "POpActor-detached" : "POpActor";

  auto expectedActorHandle = actorSystem_->middleman().remote_spawn<::caf::actor>(nodes_[nodeId],
                                                                                  actorTypeName,
                                                                                  args,
                                                                                  remoteSpawnTout);
  if (!expectedActorHandle) {
    throw std::runtime_error(fmt::format("Failed to remote-spawn operator actor '{}': {}",
                                         op->name(),
                                         to_string(expectedActorHandle.error())));
  }
  return *expectedActorHandle;
}

bool Execution::useDetached(const shared_ptr<PhysicalOp> &op) {
  // Need the following operators to be "detached" to not block others while loading data or potentially causes deadlock.
  // Don't run more S3Get requests in parallel than # cores, earlier testing showed this did not help as S3Get
  // already utilizes the full network bandwidth with #cores requests whereas S3Select does not when
  // selectivity is low.
  return op->getType() == POpType::LOCAL_FILE_SCAN
         || op->getType() == POpType::REMOTE_FILE_SCAN
         || (op->getType() == POpType::FPDB_STORE_SUPER && (ENABLE_ADAPTIVE_PUSHDOWN || ENABLE_FILTER_BITMAP_PUSHDOWN))
         || op->getType() == POpType::FPDB_STORE_TABLE_CACHE_LOAD
         || op->getType() == POpType::S3_SELECT;
}

void Execution::sendConnect(const POpDirectoryEntry &opEntry, const ::caf::scoped_actor &sender) {
  vector<POpConnection> opConnections;

  // for adaptive exec, do as follows:
  //  - for sink to consume, send consumers but no producers, and clear old connections
  //  - for sink to produce, do regularly
  //  - for sink that neither to consume nor to produce, noop
  //  - for non-sink, do regularly
  bool clear = false;
  bool addProducers = true;
  if (adaptSt_.isAdapt_ && opEntry.getDef()->getType() == POpType::ADAPT_SINK &&
     !adaptSt_.adaptPhysicalPlan_->isSinkToProduce(opEntry.getDef()->name())) {
    if (adaptSt_.adaptPhysicalPlan_->consumeAllSink()) {
      clear = true;
      addProducers = false;
    } else {
      if (adaptSt_.adaptPhysicalPlan_->isSinkToConsume(opEntry.getDef()->name())) {
        clear = true;
        addProducers = false;
      } else {
        // sink ops that are not consumed in this stage
        return;
      }
    }
  }

  // prepare connections and send message
  if (addProducers) {
    for (const auto &producer: opEntry.getDef()->producers()) {
      auto expEntry = opDirectory_.get(producer);
      if (!expEntry.has_value()) {
        throw runtime_error(expEntry.error());
      }
      auto producerHandle = (*expEntry).getActorHandle();
      opConnections.emplace_back(producer,
                                 producerHandle,
                                 POpRelationshipType::Producer,
                                 (*expEntry).getDef()->getNodeId());
    }
  }
  for (const auto &consumer: opEntry.getDef()->consumers()) {
    auto expEntry = opDirectory_.get(consumer);
    if (!expEntry.has_value()) {
      throw runtime_error(expEntry.error());
    }
    auto consumerHandle = (*expEntry).getActorHandle();
    opConnections.emplace_back(consumer,
                               consumerHandle,
                               POpRelationshipType::Consumer,
                               (*expEntry).getDef()->getNodeId());
  }
  auto cm = make_shared<message::ConnectMessage>(opConnections, clear, ExecutionRootActorName);
  sender->anon_send(opEntry.getActorHandle(), Envelope(cm));
}

void Execution::sendStart(const POpDirectoryEntry &opEntry, const ::caf::scoped_actor &sender) {
  shared_ptr<Message> msg;

  // for adaptive exec, do as follows:
  //  - for sink to consume, send "AdaptResumeMessage"
  //  - for sink to produce, send "StartMessage"
  //  - for sink that neither to consume nor to produce, noop
  //  - for non-sink, send "StartMessage"
  bool sendStart = true;
  bool adaptPreserve = false;
  if (adaptSt_.isAdapt_ && opEntry.getDef()->getType() == POpType::ADAPT_SINK &&
     !adaptSt_.adaptPhysicalPlan_->isSinkToProduce(opEntry.getDef()->name())) {
    if (adaptSt_.adaptPhysicalPlan_->consumeAllSink()) {
      sendStart = false;
    } else {
      if (adaptSt_.adaptPhysicalPlan_->isSinkToConsume(opEntry.getDef()->name(), &adaptPreserve)) {
        sendStart = false;
      } else {
        // sink ops that are not consumed in this stage
        return;
      }
    }
  }

  // prepare and send message
  if (sendStart) {
    msg = make_shared<message::StartMessage>(ExecutionRootActorName);
  } else {
    msg = make_shared<message::AdaptResumeMessage>(adaptPreserve, ExecutionRootActorName);
  }
  sender->anon_send(opEntry.getActorHandle(), Envelope(msg));
}

void Execution::write_graph(const string &file) {
  auto gvc = gvContext();

  auto graph = agopen(const_cast<char *>(string("Execution Plan").c_str()), Agstrictdirected, nullptr);

  // Init attributes
  agattr(graph, AGNODE, const_cast<char *>("fixedsize"), const_cast<char *>("false"));
  agattr(graph, AGNODE, const_cast<char *>("shape"), const_cast<char *>("ellipse"));
  agattr(graph, AGNODE, const_cast<char *>("label"), const_cast<char *>("<not set>"));
  agattr(graph, AGNODE, const_cast<char *>("fontname"), const_cast<char *>("Arial"));
  agattr(graph, AGNODE, const_cast<char *>("fontsize"), const_cast<char *>("8"));

  // Add all the nodes
  for (const auto &op: this->opDirectory_) {
    // replace invalid chars
    string nodeName = op.second.getDef()->name();
    std::replace(nodeName.begin(), nodeName.end(), '<', '|');
    std::replace(nodeName.begin(), nodeName.end(), '>', '|');
    auto node = agnode(graph, const_cast<char *>(nodeName.c_str()), true);

    agset(node, const_cast<char *>("shape"), const_cast<char *>("plaintext"));

    string nodeLabel = "<table border='1' cellborder='0' cellpadding='5'>"
                            "<tr><td align='left'><b>" + op.second.getDef()->getTypeString() + "</b></td></tr>"
                                                                                         "<tr><td align='left'>"
                            + nodeName + "</td></tr>"
                                                           "</table>";
    char *htmlNodeLabel = agstrdup_html(graph, const_cast<char *>(nodeLabel.c_str()));
    agset(node, const_cast<char *>("label"), htmlNodeLabel);
    agstrfree(graph, htmlNodeLabel);
  }

  // Add all the edges
  for (const auto &op: this->opDirectory_) {
    // replace invalid chars
    string nodeName = op.second.getDef()->name();
    std::replace(nodeName.begin(), nodeName.end(), '<', '|');
    std::replace(nodeName.begin(), nodeName.end(), '>', '|');
    auto opNode = agfindnode(graph, (char *)(nodeName.c_str()));
    for (auto c: op.second.getDef()->consumers()) {
      // replace invalid chars
      std::replace(c.begin(), c.end(), '<', '|');
      std::replace(c.begin(), c.end(), '>', '|');
      auto consumerOpNode = agfindnode(graph, (char *)(c.c_str()));
      agedge(graph, opNode, consumerOpNode, const_cast<char *>(string("Edge").c_str()), true);
    }
  }

  const filesystem::path &path = filesystem::path(file);
  if (!filesystem::exists(path.parent_path())) {
    throw runtime_error("Could not open file '" + file + "' for writing. Parent directory does not exist");
  } else {
    FILE *outFile = fopen(file.c_str(), "w");
    if (outFile == nullptr) {
      throw runtime_error("Could not open file '" + file + "' for writing. Errno: " + to_string(errno));
    }

    gvLayout(gvc, graph, "dot");
    gvRender(gvc, graph, "svg", outFile);

    fclose(outFile);

    gvFreeLayout(gvc, graph);
    agclose(graph);
    gvFreeContext(gvc);
  }
}

long Execution::getQueryId() const {
  return queryId_;
}

long Execution::getElapsedTime() {
  if (startTime_.time_since_epoch().count() == 0)
    throw runtime_error("Execution time unavailable, query has not been started");
  if (stopTime_.time_since_epoch().count() == 0)
    throw runtime_error("Execution time unavailable, query has not been stopped");

  return chrono::duration_cast<chrono::nanoseconds>(stopTime_ - startTime_).count();
}

shared_ptr<PhysicalOp> Execution::getPhysicalOp(const string &name) {
  const auto expectedEntry = opDirectory_.get(name);
  if (!expectedEntry.has_value()) {
    throw runtime_error(expectedEntry.error());
  }
  return expectedEntry.value().getDef();
}

physical::s3::S3SelectScanStats Execution::getAggregateS3SelectScanStats() {
  physical::s3::S3SelectScanStats aggregateS3SelectScanStats = {0, 0, 0, 0 , 0, 0 ,0, 0}; // initialize all fields to 0
  for (const auto &entry: opDirectory_) {
    const auto op = entry.second.getDef();
    if (op->getType() == POpType::S3_GET || op->getType() == POpType::S3_SELECT) {
      auto s3SelectScanOp = static_pointer_cast<physical::s3::S3SelectScanAbstractPOp>(op);
      physical::s3::S3SelectScanStats currentS3SelectScanStats = s3SelectScanOp->getS3SelectScanStats();

      // Add these s3SelectScanStats to our aggregate stats
      aggregateS3SelectScanStats.processedBytes += currentS3SelectScanStats.processedBytes;
      aggregateS3SelectScanStats.returnedBytes += currentS3SelectScanStats.returnedBytes;
      aggregateS3SelectScanStats.outputBytes += currentS3SelectScanStats.outputBytes;
      aggregateS3SelectScanStats.numRequests += currentS3SelectScanStats.numRequests;
      aggregateS3SelectScanStats.getTransferTimeNS += currentS3SelectScanStats.getTransferTimeNS;
      aggregateS3SelectScanStats.getConvertTimeNS += currentS3SelectScanStats.getConvertTimeNS;
      aggregateS3SelectScanStats.selectTransferTimeNS += currentS3SelectScanStats.selectTransferTimeNS;
      aggregateS3SelectScanStats.selectConvertTimeNS += currentS3SelectScanStats.selectConvertTimeNS;
    }
  }
  return aggregateS3SelectScanStats;
}

std::tuple<size_t, size_t, size_t> Execution::getFilterTimeNSInputOutputBytes() {
  size_t timeNS = 0;
  size_t inputBytes = 0;
  size_t outputBytes = 0;
  for (const auto &entry: opDirectory_) {
    const auto op = entry.second.getDef();
    if (op->getType() == POpType::FILTER) {
      auto filterOp = static_pointer_cast<filter::FilterPOp>(entry.second.getDef());

      timeNS += filterOp->getFilterTimeNS();
      inputBytes += filterOp->getFilterInputBytes();
      outputBytes += filterOp->getFilterOutputBytes();
    }
  }
  return {timeNS, inputBytes, outputBytes};
}

string Execution::showRegularMetrics() {
  stringstream ss;
  ss << endl;
  ss << fmt::format("Metrics (id: {}) |", queryId_) << endl << endl;

  if (metrics::SHOW_OP_TIME || metrics::SHOW_OP_TYPE_TIME) {
    fetchOpExecTimes();

    if (metrics::SHOW_OP_TIME) {
      auto totalExecutionTime = getElapsedTime();
      stringstream formattedExecutionTime;
      formattedExecutionTime << totalExecutionTime << " \u33B1" << " (" << ((double) totalExecutionTime / 1000000000.0)
                             << " secs)";
      ss << left << setw(60) << "Total Execution Time ";
      ss << left << setw(60) << formattedExecutionTime.str();
      ss << endl;
      ss << endl;

      ss << left << setw(120) << "Operator Execution Times" << endl;
      ss << setfill(' ');

      ss << left << setw(120) << setfill('-') << "" << endl;
      ss << setfill(' ');

      ss << left << setw(60) << "Operator";
      ss << left << setw(10) << "Node";
      ss << left << setw(35) << "Execution Time";
      ss << left << setw(15) << "% Total Time";
      ss << endl;

      ss << left << setw(120) << setfill('-') << "" << endl;
      ss << setfill(' ');

      // op times
      for (const auto &opTime: opTimes_) {
        auto operatorName = opTime.first;
        long processingTime = opTime.second;
        auto processingFraction = (double) processingTime / (double) totalOpTime_;
        stringstream formattedProcessingTime;
        formattedProcessingTime << processingTime << " \u33B1" << " (" << ((double) processingTime / 1000000000.0)
                                << " secs)";
        stringstream formattedProcessingPercentage;
        formattedProcessingPercentage << (processingFraction * 100.0);

        // split op name if too wide
        vector<string> opNameSplit;
        uint st = 0, len = operatorName.size();
        while (st < len) {
          uint splitLen = min(60u - 1, len - st);
          opNameSplit.emplace_back(operatorName.substr(st, splitLen));
          st += splitLen;
        }

        ss << left << setw(60) << opNameSplit[0];
        ss << left << setw(10) << opDirectory_.get(operatorName).value().getDef()->getNodeId();
        ss << left << setw(35) << formattedProcessingTime.str();
        ss << left << setw(15) << formattedProcessingPercentage.str();
        ss << endl;

        // show the rest part of op name if too wide
        for (uint i = 1; i < opNameSplit.size(); ++i) {
          ss << left << setw(60) << opNameSplit[i] << endl;
        }
      }

      // network times
      for (const auto &networkTime: networkTimes_) {
        auto operatorName = networkTime.first;
        long time = networkTime.second;
        auto processingFraction = (double) time / (double) totalOpTime_;
        stringstream formattedProcessingTime;
        formattedProcessingTime << time << " \u33B1" << " (" << ((double) time / 1000000000.0) << " secs)";
        stringstream formattedProcessingPercentage;
        formattedProcessingPercentage << (processingFraction * 100.0);

        // split op name if too wide
        string opNameToShow = fmt::format("[{}] {}", std::string(NetworkTimeKey), operatorName);
        vector<string> opNameSplit;
        uint st = 0, len = opNameToShow.size();
        while (st < len) {
          uint splitLen = min(60u - 1, len - st);
          opNameSplit.emplace_back(opNameToShow.substr(st, splitLen));
          st += splitLen;
        }

        ss << left << setw(60) << opNameSplit[0];
        ss << left << setw(10) << opDirectory_.get(operatorName).value().getDef()->getNodeId();
        ss << left << setw(35) << formattedProcessingTime.str();
        ss << left << setw(15) << formattedProcessingPercentage.str();
        ss << endl;

        // show the rest part of op name if too wide
        for (uint i = 1; i < opNameSplit.size(); ++i) {
          ss << left << setw(60) << opNameSplit[i] << endl;
        }
      }

      ss << left << setw(120) << setfill('-') << "" << endl;
      ss << setfill(' ');

      stringstream formattedProcessingTime;
      formattedProcessingTime << totalOpTime_ << " \u33B1" << " (" << ((double) totalOpTime_ / 1000000000.0)
                              << " secs)";
      ss << left << setw(70) << "Total ";
      ss << left << setw(35) << formattedProcessingTime.str();
      ss << left << setw(15) << "100.0";
      ss << endl;
    }

    if (metrics::SHOW_OP_TYPE_TIME) {
      if (metrics::SHOW_OP_TIME) {
        ss << endl;
      }
      for (auto const &opTypeTime: opTypeTimes_) {
        // put network at the end
        if (opTypeTime.first == std::string(NetworkTimeKey)) {
          continue;
        }
        stringstream formattedOpTypeTime;
        formattedOpTypeTime << ((double) opTypeTime.second / 1000000000.0) << " secs";
        stringstream formattedOpTypePercentage;
        formattedOpTypePercentage << ((double) opTypeTime.second / (double) totalOpTime_) * 100.0;
        ss << left << setw(70) << opTypeTime.first;
        ss << left << setw(35) << formattedOpTypeTime.str();
        ss << left << setw(15) << formattedOpTypePercentage.str();
        ss << endl;
      }

      ss << left << setw(120) << setfill('-') << "" << endl;
      ss << setfill(' ');

      stringstream formattedProcessingTime;
      formattedProcessingTime << totalOpTime_ << " \u33B1" << " (" << ((double) totalOpTime_ / 1000000000.0)
                              << " secs)";
      ss << left << setw(70) << "Total ";
      ss << left << setw(35) << formattedProcessingTime.str();
      ss << left << setw(15) << "100.0";
      ss << endl;
    }
  }

  if (metrics::SHOW_SCAN_METRICS) {
    physical::s3::S3SelectScanStats s3SelectScanStats = getAggregateS3SelectScanStats();

    stringstream formattedProcessedBytes;
    formattedProcessedBytes << s3SelectScanStats.processedBytes << " B" << " ("
                            << ((double) s3SelectScanStats.processedBytes / 1024.0 / 1024.0 / 1024.0) << " GB)";
    stringstream formattedReturnedBytes;
    formattedReturnedBytes << s3SelectScanStats.returnedBytes << " B" << " ("
                           << ((double) s3SelectScanStats.returnedBytes / 1024.0 / 1024.0 / 1024.0) << " GB)";
    stringstream formattedArrowConvertedBytes;
    formattedArrowConvertedBytes << s3SelectScanStats.outputBytes << " B" << " ("
                                 << ((double) s3SelectScanStats.outputBytes / 1024.0 / 1024.0 / 1024.0) << " GB)";

    stringstream formattedConversionOutputRate;
    if (s3SelectScanStats.getConvertTimeNS + s3SelectScanStats.selectConvertTimeNS > 0) {
      formattedConversionOutputRate << ((double) s3SelectScanStats.outputBytes / 1024.0 / 1024.0) /
                                       ((double) (s3SelectScanStats.getConvertTimeNS +
                                                  s3SelectScanStats.selectConvertTimeNS) / 1.0e9) << " MB/s/req";
    } else {
      formattedConversionOutputRate << "NA";
    }

    stringstream formattedStorageFormatToArrowSizeX;
    if (s3SelectScanStats.outputBytes > 0) {
      formattedStorageFormatToArrowSizeX
              << (double) s3SelectScanStats.returnedBytes / (double) s3SelectScanStats.outputBytes << "x";
    } else {
      formattedStorageFormatToArrowSizeX << "NA";
    }
    ss << endl;
    ss << left << setw(60) << "Processed Bytes";
    ss << left << setw(60) << formattedProcessedBytes.str();
    ss << endl;
    ss << left << setw(60) << "Returned Bytes";
    ss << left << setw(60) << formattedReturnedBytes.str();
    ss << endl;
    ss << left << setw(60) << "Arrow Converted Bytes";
    ss << left << setw(60) << formattedArrowConvertedBytes.str();
    ss << endl;
    ss << left << setw(60) << "Conversion Output Rate";
    ss << left << setw(60) << formattedConversionOutputRate.str();
    ss << endl;
    ss << left << setw(60) << "Storage/Compute Data Ratio";
    ss << left << setw(60) << formattedStorageFormatToArrowSizeX.str();
    ss << endl;

    stringstream formattedGetTransferRate;
    stringstream formattedGetConvertRate;
    stringstream formattedGetTransferConvertRate;
    if (s3SelectScanStats.getTransferTimeNS > 0 && s3SelectScanStats.getConvertTimeNS > 0) {
      formattedGetTransferRate << ((double) s3SelectScanStats.returnedBytes / 1024.0 / 1024.0) /
                                  ((double) s3SelectScanStats.getTransferTimeNS / 1.0e9) << " MB/s/req";
      formattedGetConvertRate << ((double) s3SelectScanStats.returnedBytes / 1024.0 / 1024.0) /
                                 ((double) s3SelectScanStats.getConvertTimeNS / 1.0e9) << " MB/s/req";
      formattedGetTransferConvertRate << ((double) s3SelectScanStats.returnedBytes / 1024.0 / 1024.0) /
                                         (((double) s3SelectScanStats.getTransferTimeNS +
                                         (double) s3SelectScanStats.getConvertTimeNS) / 1.0e9) << " MB/s/req";
    } else {
      formattedGetTransferRate << "NA";
      formattedGetConvertRate << "NA";
      formattedGetTransferConvertRate << "NA";
    }
    // Caf actor framework seems to converge #workers -> # cores for non detached workers, and each worker runs to
    // completion. This should approximate to per core rates rather than just per request, as one request maps to a core
    ss << left << setw(60) << "S3 GET Data Transfer Rate";
    ss << left << setw(60) << formattedGetTransferRate.str();
    ss << endl;
    ss << left << setw(60) << "S3 GET Data Convert rate";
    ss << left << setw(60) << formattedGetConvertRate.str();
    ss << endl;
    ss << left << setw(60) << "S3 GET Data Transfer + Convert rate";
    ss << left << setw(60) << formattedGetTransferConvertRate.str();
    ss << endl;

    stringstream formattedSelectTransferRate;
    stringstream formattedSelectConvertRate;
    stringstream formattedSelectTransferConvertRate;
    if (s3SelectScanStats.selectTransferTimeNS > 0 && s3SelectScanStats.selectConvertTimeNS > 0) {
      formattedSelectTransferRate << ((double) s3SelectScanStats.returnedBytes / 1024.0 / 1024.0) /
                                     ((double) (s3SelectScanStats.selectTransferTimeNS) / 1.0e9) << " MB/s/req";
      formattedSelectConvertRate << ((double) s3SelectScanStats.returnedBytes / 1024.0 / 1024.0) /
                                    ((double) s3SelectScanStats.selectConvertTimeNS / 1.0e9) << " MB/s/req";
      formattedSelectTransferConvertRate << ((double) s3SelectScanStats.returnedBytes / 1024.0 / 1024.0) /
                                            ((double) (s3SelectScanStats.selectTransferTimeNS +
                                                       s3SelectScanStats.selectConvertTimeNS) / 1.0e9) << " MB/s/req";
    } else {
      formattedSelectTransferRate << "NA";
      formattedSelectConvertRate << "NA";
      formattedSelectTransferConvertRate << "NA";
    }

    // FIXME: This only works if the query is entirely pushdown (it only uses S3 Select and never GET), as the bytes
    //        transferred is grouped together for select and get requests, so they are not differentiated
    //        If we decide to eventually allow GET and Select requests to be in the same query for a mode we will need
    //        to have a way to differentiate this (this isn't the case for now though so it is fine)
    //        Also note that some storage backends such as Airmettle don't tell use processedBytes, in which case
    //        we estimate this value with the select range.
    stringstream formattedS3SelectSelectivityPercent;
    if (s3SelectScanStats.returnedBytes > 0 && s3SelectScanStats.processedBytes &&
        s3SelectScanStats.selectTransferTimeNS > 0 && s3SelectScanStats.getConvertTimeNS == 0) {
      formattedS3SelectSelectivityPercent << (double) s3SelectScanStats.returnedBytes /
                                             (double) s3SelectScanStats.processedBytes * 100 << "%";
    } else {
      formattedS3SelectSelectivityPercent << "NA";
    }
    ss << left << setw(60) << "Appx S3 Select Data Transfer Rate";
    ss << left << setw(60) << formattedSelectTransferRate.str();
    ss << endl;
    ss << left << setw(60) << "S3 Select Data Convert rate";
    ss << left << setw(60) << formattedSelectConvertRate.str();
    ss << endl;
    ss << left << setw(60) << "S3 Select Data Transfer + Convert rate";
    ss << left << setw(60) << formattedSelectTransferConvertRate.str();
    ss << endl;
    ss << left << setw(60) << "S3 Selectivity";
    ss << left << setw(60) << formattedS3SelectSelectivityPercent.str();
    ss << endl;

    auto filterTimeNSInputOutputBytes = getFilterTimeNSInputOutputBytes();
    size_t filterTimeNS = get<0>(filterTimeNSInputOutputBytes);
    size_t filterInputBytes = get<1>(filterTimeNSInputOutputBytes);
    size_t filterOutputBytes = get<2>(filterTimeNSInputOutputBytes);
    stringstream formattedLocalFilterRateGBs;
    stringstream formattedLocalFilterGB;
    stringstream formattedLocalFilterSelectivity;
    if (filterTimeNS > 0) {
      double filterGB = ((double) filterInputBytes / 1024.0 / 1024.0 / 1024.0);
      formattedLocalFilterRateGBs << filterGB / ((double) filterTimeNS / 1.0e9) << " GB/s/req";
      formattedLocalFilterGB << filterGB << " GB";
      formattedLocalFilterSelectivity << ((double) filterOutputBytes / (double) filterInputBytes) * 100 << "%";
    } else {
      formattedLocalFilterRateGBs << "NA";
      formattedLocalFilterGB << "NA";
      formattedLocalFilterSelectivity << "NA";
    }
    // Caf actor framework seems to converge #workers -> # cores, and each worker runs to completion
    // so this should approximate to per core rates rather than just per request, as one request maps to a core
    ss << left << setw(60) << "Local filter rate";
    ss << left << setw(60) << formattedLocalFilterRateGBs.str();
    ss << endl;
    ss << left << setw(60) << "Local filter bytes";
    ss << left << setw(60) << formattedLocalFilterGB.str();
    ss << endl;
    ss << left << setw(60) << "Local filter selectivity (bytes)";
    ss << left << setw(60) << formattedLocalFilterSelectivity.str();
    ss << endl;
  }

  return ss.str();
}

#if SHOW_DEBUG_METRICS == true
string Execution::showDebugMetrics() {
  stringstream ss;
  ss << endl;
  ss << fmt::format("Debug Metrics (id: {}) |", queryId_) << endl << endl;

  if (metrics::SHOW_NETWORK_METRICS) {
    ss << endl << "Network Metrics |" << endl << endl;
    stringstream formattedBytesFromStore;
    int64_t bytesFromStore = debugMetrics_.getNetworkMetrics().getBytesFromStore();
    formattedBytesFromStore << bytesFromStore << " B" << " ("
                            << ((double) bytesFromStore / 1024.0 / 1024.0 / 1024.0) << " GB)";

    ss << left << setw(60) << "Bytes transferred from store";
    ss << left << setw(60) << formattedBytesFromStore.str();
    ss << endl;

    stringstream formattedBytesToStore;
    int64_t bytesToStore = debugMetrics_.getNetworkMetrics().getBytesToStore();
    formattedBytesToStore << bytesToStore << " B" << " ("
                          << ((double) bytesToStore / 1024.0 / 1024.0 / 1024.0) << " GB)";

    ss << left << setw(60) << "Bytes transferred to store";
    ss << left << setw(60) << formattedBytesToStore.str();
    ss << endl;

    stringstream formattedBytesInterCompute;
    int64_t bytesInterCompute = debugMetrics_.getNetworkMetrics().getBytesInterCompute();
    formattedBytesInterCompute << bytesInterCompute << " B" << " ("
                               << ((double) bytesInterCompute / 1024.0 / 1024.0 / 1024.0) << " GB)";

    ss << left << setw(60) << "Bytes transferred across compute nodes";
    ss << left << setw(60) << formattedBytesInterCompute.str();
    ss << endl;

    stringstream formattedBytesRemote;
    int64_t bytesRemote = bytesFromStore + bytesToStore + bytesInterCompute;
    formattedBytesRemote << bytesRemote << " B" << " ("
                         << ((double) bytesRemote / 1024.0 / 1024.0 / 1024.0) << " GB)";

    ss << left << setw(60) << "Bytes transferred totally";
    ss << left << setw(60) << formattedBytesRemote.str();
    ss << endl;
  }

  if (metrics::SHOW_PRED_TRANS_METRICS) {
    auto metrics = debugMetrics_.getPredTransMetrics().getMetrics();
    ss << endl << "Predicate Transfer Metrics |" << endl;

    if (!metrics.empty()) {
      fetchOpExecTimes();

      stringstream formattedPredTransOpTime;
      formattedPredTransOpTime << ((double) totalPredTransOpTime_ / 1000000000.0) << " secs" << " ("
                               << setprecision(3)
                               << ((double) totalPredTransOpTime_) * 100 / ((double) totalOpTime_) << "%)";
      stringstream formattedJoinOpTime;
      formattedJoinOpTime << ((double) totalJoinOpTime_ / 1000000000.0) << " secs" << " ("
                          << setprecision(3)
                          << ((double) totalJoinOpTime_) * 100 / ((double) totalOpTime_) << "%)";
      long totalOtherOpTime = totalOpTime_ - totalPredTransOpTime_ - totalJoinOpTime_;
      stringstream formattedOtherOpTime;
      formattedOtherOpTime << ((double) totalOtherOpTime / 1000000000.0) << " secs" << " ("
                           << setprecision(3)
                           << ((double) totalOtherOpTime) * 100 / ((double) totalOpTime_) << "%)";

      ss << endl;
      ss << left << setw(60) << "Predicate Transfer Time";
      ss << formattedPredTransOpTime.str();

      ss << endl;
      ss << left << setw(60) << "Join Time";
      ss << formattedJoinOpTime.str();

      ss << endl;
      ss << left << setw(60) << "Other Time";
      ss << formattedOtherOpTime.str();
      ss << endl;

      int64_t bytesInterCompute = debugMetrics_.getNetworkMetrics().getBytesInterCompute();
      if (bytesInterCompute > 0) {
        stringstream formattedPredTransInterComputeBytes;
        formattedPredTransInterComputeBytes
                << totalPredTransInterComputeBytes_ << " B" << " ("
                << ((double) totalPredTransInterComputeBytes_ / 1024.0 / 1024.0 / 1024.0) << " GB)";
        stringstream formattedJoinInterComputeBytes;
        formattedJoinInterComputeBytes
                << totalJoinInterComputeBytes_ << " B" << " ("
                << ((double) totalJoinInterComputeBytes_ / 1024.0 / 1024.0 / 1024.0) << " GB)";
        stringstream formattedOtherInterComputeBytes;
        int64_t totalOtherInterComputeBytes =
                bytesInterCompute - totalPredTransInterComputeBytes_ - totalJoinInterComputeBytes_;
        formattedOtherInterComputeBytes
                << totalOtherInterComputeBytes << " B" << " ("
                << ((double) totalOtherInterComputeBytes / 1024.0 / 1024.0 / 1024.0) << " GB)";

        ss << endl;
        ss << left << setw(60) << "[Bytes across compute nodes] Predicate Transfer";
        ss << formattedPredTransInterComputeBytes.str();

        ss << endl;
        ss << left << setw(60) << "[Bytes across compute nodes] Join";
        ss << formattedJoinInterComputeBytes.str();

        ss << endl;
        ss << left << setw(60) << "[Bytes across compute nodes] Other";
        ss << formattedOtherInterComputeBytes.str();
        ss << endl;
      }

      for (const auto &unit: metrics) {
        ss << endl;
        ss << left << setw(60) << "Prephysical Op ID";
        ss << "[" << unit.prePOpId_ << "]";
        ss << endl;

        ss << left << setw(60) << "Table";
        ss << unit.table_;
        ss << endl;

        ss << left << setw(60) << "Collector POp Type";
        ss << unit.collectorPOpTypeStr_;
        ss << endl;

        ss << left << setw(60) << "Predicate Transfer Type";
        ss << metrics::PredTransMetrics::PTMetricsUnitTypeToStr(unit.type_);
        ss << endl;

        ss << left << setw(60) << "Rows after Predicate Transfer";
        ss << unit.numRows_;
        ss << endl;

        ss << left << setw(60) << "Schema" << endl;
        auto splitStr = fpdb::util::split(unit.schema_->ToString(), "\n");
        for (const auto &str: splitStr) {
          ss << "- " << str << endl;
        }
      }
    }
  }

  if (metrics::SHOW_PRED_TRANS_CS_METRICS) {
    auto metrics = debugMetrics_.getPredTransCSMetrics().getMetrics();
    ss << endl << "Predicate Transfer Case Study Metrics |" << endl;
    fetchOpExecTimes();

    for (const auto &unit: metrics) {
      ss << endl;
      ss << left << setw(60) << "Dir-Step";
      ss << fmt::format("{}-{}", unit.info_.forward_ ? "F" : "B", unit.info_.step_);
      ss << endl;

      ss << left << setw(60) << "Src -> Dst";
      ss << fmt::format("{}[{}] {} {}[{}]",
                        unit.info_.srcTable_, unit.info_.srcPrePOpId_,
                        unit.info_.forward_ ? "->" : "<-",
                        unit.info_.dstTable_, unit.info_.dstPrePOpId_);
      ss << endl;

      ss << left << setw(60) << "Dist PT Type";
      ss << unit.info_.distPTType_;
      ss << endl;

      ss << left << setw(60) << "Filtering (Rows In, Rows Out)";
      ss << unit.numRowsIn_ << ", " << unit.numRowsOut_;
      ss << endl;

      ss << left << setw(60) << "Filtering (Selectivity)";
      ss << (unit.numRowsIn_ == 0 ? 0 : 1.0 * unit.numRowsOut_ / unit.numRowsIn_);
      ss << endl;

      ss << left << setw(60) << "Bloom Filter (Size)";
      ss << unit.bfSize_ << " bytes";
      ss << endl;

      long queryTime = getElapsedTime();
      ss << left << setw(60) << "Bloom Filter (Build Time, with Network)";
      ss << fmt::format("{} ns (proportional to {:.2f} ns in query time)",
                        unit.bfBuildTime_, 1.0 * unit.bfBuildTime_ / totalOpTime_ * queryTime);
      ss << endl;

      ss << left << setw(60) << "Bloom Filter (Probe Time, with Network)";
      ss << fmt::format("{} ns (proportional to {:.2f} ns in query time)",
                        unit.bfProbeTime_, 1.0 * unit.bfProbeTime_ / totalOpTime_ * queryTime);
      ss << endl;
    }
  }

  if (metrics::SHOW_HASH_JOIN_METRICS) {
    ss << endl << "Hash Join Metrics |" << endl;

    fetchOpExecTimes();

    // for individual join, currently only single-node exec supports this metrics
    if (!isDistributed_) {
      ss << left << setw(110) << setfill('-') << "" << endl;
      ss << setfill(' ');
      ss << left << setw(65) << "Operator";
      ss << left << setw(15) << "Time (ms)";
      ss << left << setw(15) << "Build Size";
      ss << left << setw(15) << "Probe Size";
      ss << endl;
      ss << left << setw(110) << setfill('-') << "" << endl;
      ss << setfill(' ');

      for (auto &entry: opDirectory_) {
        auto operatorName = entry.first;
        auto op = entry.second.getDef();
        if (op->getType() == POpType::HASH_JOIN_ARROW) {
          long processingTime = opTimes_[operatorName];
          auto typedOp = std::static_pointer_cast<join::HashJoinArrowPOp>(op);
          ss << left << setw(65) << operatorName;
          ss << left << setw(15) << setprecision(3) << ((double) processingTime / 1000000.0);
          ss << left << setw(15) << typedOp->getNumRowsBuild();
          ss << left << setw(15) << typedOp->getNumRowsProbe();
          ss << endl;
        }
      }

      ss << left << setw(110) << setfill('-') << "" << endl;
      ss << setfill(' ');
      ss << endl;
    }

    // for total
    int64_t totalNumBuild = debugMetrics_.getHashJoinMetrics().getNumHtBuild();
    int64_t totalNumProbe = debugMetrics_.getHashJoinMetrics().getNumHtProbe();

    ss << left << setw(60) << "Total num hash table build";
    ss << totalNumBuild << endl;
    ss << left << setw(60) << "Total num hash table probe";
    ss << totalNumProbe << endl;
  }

  if (metrics::SHOW_BLOOM_FILTER_METRICS) {
    ss << endl << "Bloom Filter Metrics |" << endl;

    fetchOpExecTimes();

    // for individual bf, currently only single-node exec supports this metrics
    if (!isDistributed_) {
      ss << left << setw(95) << setfill('-') << "" << endl;
      ss << setfill(' ');
      ss << left << setw(65) << "Operator";
      ss << left << setw(15) << "Time (ms)";
      ss << left << setw(15) << "Input Size";
      ss << endl;
      ss << left << setw(95) << setfill('-') << "" << endl;
      ss << setfill(' ');

      for (auto &entry: opDirectory_) {
        auto operatorName = entry.first;
        auto op = entry.second.getDef();
        if (op->getType() == POpType::BLOOM_FILTER_CREATE || op->getType() == POpType::BLOOM_FILTER_USE) {
          long processingTime = opTimes_[operatorName];
          ss << left << setw(65) << operatorName;
          ss << left << setw(15) << setprecision(3) << ((double) processingTime / 1000000.0);
          if (op->getType() == POpType::BLOOM_FILTER_CREATE) {
            int64_t numRowsInput = std::static_pointer_cast<bloomfilter::BloomFilterCreatePOp>(op)->getNumRowsInput();
            ss << left << setw(15) << numRowsInput;
          } else {
            int64_t numRowsInput = std::static_pointer_cast<bloomfilter::BloomFilterUsePOp>(op)->getNumRowsInput();
            ss << left << setw(15) << numRowsInput;
          }
          ss << endl;
        }
      }

      ss << left << setw(95) << setfill('-') << "" << endl;
      ss << setfill(' ');
      ss << endl;
    }

    // for total
    int64_t totalNumInsert = debugMetrics_.getHashJoinMetrics().getNumBfInsert();
    int64_t totalNumFind = debugMetrics_.getHashJoinMetrics().getNumBfFind();

    ss << left << setw(60) << "Total num BF insert";
    ss << totalNumInsert << endl;
    ss << left << setw(60) << "Total num BF find";
    ss << totalNumFind << endl;
  }

  if (ENABLE_ADAPTIVE_PUSHDOWN && metrics::SHOW_NUM_PUSHDOWN_FALL_BACK) {
    int numFPDBStoreSuperPOps = 0;
    for (const auto &opIt: physicalPlan_->getPhysicalOps()) {
      if (opIt.second->getType() == POpType::FPDB_STORE_SUPER) {
        ++numFPDBStoreSuperPOps;
      }
    }

    stringstream formattedNumPushdownFallBack;
    int numPushdownFallBack = debugMetrics_.getNumPushdownFallBack();
    formattedNumPushdownFallBack << numPushdownFallBack << " / " << numFPDBStoreSuperPOps;

    ss << left << setw(60) << "Num pushdown fall back / num total pushdown req";
    ss << left << setw(60) << formattedNumPushdownFallBack.str();
    ss << endl;
  }

  return ss.str();
}

const metrics::DebugMetrics &Execution::getDebugMetrics() const {
  return debugMetrics_;
}
#endif

void Execution::fetchOpExecTimes() {
  if (isOpTimeFetched_) {
    return;
  }
  for (auto &entry : opDirectory_) {
    // get processing time (excluding network time)
    (*rootActor_)->request(entry.second.getActorHandle(), ::caf::infinite, GetProcessingTimeAtom_v).receive(
            [&](long processingTime) {
              totalOpTime_ += processingTime;
              opTimes_[entry.first] = processingTime;
              opTypeTimes_[entry.second.getDef()->getTypeString()] += processingTime;
#if SHOW_DEBUG_METRICS == true
              switch (entry.second.getDef()->getPTPhaseType()) {
                case metrics::PredTransMetrics::PRED_TRANS_PHASE: {
                  totalPredTransOpTime_ += processingTime;
                  break;
                }
                case metrics::PredTransMetrics::JOIN_PHASE: {
                  totalJoinOpTime_ += processingTime;
                  break;
                }
                default: {
                  break;
                }
              }
#endif
            },
            [&](const ::caf::error& error){
              throw runtime_error(to_string(error));
            });

    // get network time
    (*rootActor_)->request(entry.second.getActorHandle(), ::caf::infinite, GetNetworkTimeAtom_v).receive(
            [&](long networkTime) {
              // only record those which really incur a network traffic
              if (networkTime > 0) {
                totalOpTime_ += networkTime;
                networkTimes_[entry.first] = networkTime;
                opTypeTimes_[std::string(NetworkTimeKey)] += networkTime;
                opTypeTimes_[fmt::format("[{}] {}", std::string(NetworkTimeKey),
                                         entry.second.getDef()->getTypeString())] += networkTime;
#if SHOW_DEBUG_METRICS == true
                switch (entry.second.getDef()->getPTPhaseType()) {
                  case metrics::PredTransMetrics::PRED_TRANS_PHASE: {
                    totalPredTransOpTime_ += networkTime;
                    break;
                  }
                  case metrics::PredTransMetrics::JOIN_PHASE: {
                    totalJoinOpTime_ += networkTime;
                    break;
                  }
                  default: {
                    break;
                  }
                }
#endif
              }
            },
            [&](const ::caf::error& error){
              throw runtime_error(to_string(error));
            });
  }
  isOpTimeFetched_ = true;
}

}
