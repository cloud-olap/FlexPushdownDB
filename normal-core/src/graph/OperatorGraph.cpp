//
// Created by matt on 7/7/20.
//

#include "normal/core/graph/OperatorGraph.h"

#include <cassert>

#include <caf/all.hpp>
#include <filesystem>
#include <utility>
#include <graphviz/gvc.h>

#include <normal/core/ATTIC/Actors.h>
#include <normal/core/OperatorDirectoryEntry.h>
#include <normal/core/Globals.h>
#include <normal/pushdown/file/FileScan2.h>
#include <normal/pushdown/s3/S3Select.h>
#include <normal/pushdown/s3/S3Get.h>
#include <normal/pushdown/filter/Filter.h>
#include <normal/core/message/ConnectMessage.h>

using namespace normal::core::graph;
using namespace normal::core;
using namespace normal::pushdown;
using namespace normal::pushdown::s3;
using namespace normal::pushdown::file;

graph::OperatorGraph::OperatorGraph(long id, const std::shared_ptr<OperatorManager> &operatorManager) :
	id_(id),
	operatorManager_(operatorManager) {
  rootActor_ = std::make_shared<caf::scoped_actor>(*operatorManager->getActorSystem());
}

std::shared_ptr<OperatorGraph> graph::OperatorGraph::make(const std::shared_ptr<OperatorManager>& operatorManager) {
  return std::make_shared<OperatorGraph>(operatorManager->nextQueryId(), operatorManager);
}

void graph::OperatorGraph::put(const std::shared_ptr<Operator> &def) {
  assert(def);
  operatorDirectory_.insert(OperatorDirectoryEntry(def,
												   nullptr,
												   false));
}

void graph::OperatorGraph::start() {

  startTime_ = std::chrono::steady_clock::now();

  // Mark all the operators as incomplete
  operatorDirectory_.setIncomplete();

  // Connect the actors
  for (const auto &element: operatorDirectory_) {
    auto entry = element.second;

    std::vector<OperatorConnection> operatorConnections;
    for (const auto &producer: element.second.getDef()->producers()) {
      auto producerHandle = operatorDirectory_.get(producer.first).value().getActorHandle();
      operatorConnections.emplace_back(producer.first, producerHandle, OperatorRelationshipType::Producer);
    }
    for (const auto &consumer: element.second.getDef()->consumers()) {
      auto consumerHandle = operatorDirectory_.get(consumer.first).value().getActorHandle();
      operatorConnections.emplace_back(consumer.first, consumerHandle, OperatorRelationshipType::Consumer);
    }

    auto cm = std::make_shared<message::ConnectMessage>(operatorConnections, GraphRootActorName);
    (*rootActor_)->anon_send(element.second.getActorHandle(), normal::core::message::Envelope(cm));
  }

  // Start the actors
  for (const auto &element: operatorDirectory_) {
    auto entry = element.second;
    auto sm = std::make_shared<message::StartMessage>(GraphRootActorName);
    (*rootActor_)->anon_send(element.second.getActorHandle(), normal::core::message::Envelope(sm));
  }
}

void graph::OperatorGraph::join() {

  SPDLOG_DEBUG("Waiting for all operators to complete");

  auto handle_err = [&](const caf::error &err) {
	throw std::runtime_error(to_string(err));
  };

  bool allComplete = false;
  (*rootActor_)->receive_while([&] { return !allComplete; })(
	  [&](const normal::core::message::Envelope &msg) {
      SPDLOG_DEBUG("Query root actor received message  |  query: '{}', messageKind: '{}', from: '{}'",
             this->getId(), msg.message().type(), msg.message().sender());

      this->operatorDirectory_.setComplete(msg.message().sender());

      allComplete = this->operatorDirectory_.allComplete();
	  },
	  handle_err);

  // FIXME: Massive hack
  // Since almost all tests/benchmarks etc look at the collate operator to get the final tuples. Here we move them from
  // Collate2 to the old Collate operator.
//  (*rootActor_)->request(collateActorHandle_, caf::infinite, GetTupleSetAtom::value).receive(
//	  [&](const tl::expected<std::shared_ptr<TupleSet>, std::string> &expectedTupleSet) {
//		legacyCollateOperator_->setTuples(expectedTupleSet.value());
//	  },
//	  [&](const caf::error&  error){
//		throw std::runtime_error(to_string(error));
//	  });

  stopTime_ = std::chrono::steady_clock::now();
}

void graph::OperatorGraph::boot() {

  // Create the operator actors
  for (auto &element: operatorDirectory_) {
	auto op = element.second.getDef();
	if(op->getType() == "FileScan"){
	  auto fileScanOp = std::static_pointer_cast<FileScan>(op);
	  auto actorHandle = operatorManager_.lock()->getActorSystem()->spawn(FileScanFunctor,
																		  fileScanOp->name(),
																		  fileScanOp->getKernel()->getPath(),
																		  fileScanOp->getKernel()->getFileType().value(),
																		  fileScanOp->getColumnNames(),
																		  fileScanOp->getKernel()->getStartPos(),
																		  fileScanOp->getKernel()->getFinishPos(),
																		  fileScanOp->getQueryId(),
																		  *rootActor_,
																		  operatorManager_.lock()->getSegmentCacheActor(),
																		  fileScanOp->isScanOnStart()
	  );
	  if (!actorHandle)
		  throw std::runtime_error(fmt::format("Failed to spawn operator actor '{}'", op->name()));
	  element.second.setActorHandle(caf::actor_cast<caf::actor>(actorHandle));
	}
//	else if(op->getType() == "Collate"){
//	  legacyCollateOperator_ = std::static_pointer_cast<Collate>(op);
//	  collateActorHandle_  = operatorManager_.lock()->getActorSystem()->spawn(CollateFunctor,
//																			  legacyCollateOperator_->name(),
//																			  legacyCollateOperator_->getQueryId(),
//																		  *rootActor_,
//																		  operatorManager_.lock()->getSegmentCacheActor()
//	  );
//	  if (!collateActorHandle_)
//		throw std::runtime_error(fmt::format("Failed to spawn operator actor '{}'", op->name()));
//	  element.second.setActorHandle(caf::actor_cast<caf::actor>(collateActorHandle_));
//	}
	else {
	  auto ctx = std::make_shared<normal::core::OperatorContext>(*rootActor_, operatorManager_.lock()->getSegmentCacheActor());
	  op->create(ctx);
    // Don't run more S3Get requests in parallel than # cores, earlier testing showed this did not help as S3Get
    // already utilizes the full network bandwidth with #cores requests whereas S3Select does not when
    // selectivity is low.
	  if (op->getType() == "S3Select" || op->getType() == "S3Get") {
	    auto actorHandle = operatorManager_.lock()->getActorSystem()->spawn<normal::core::OperatorActor, detached>(op);
	    if (!actorHandle)
        throw std::runtime_error(fmt::format("Failed to spawn operator actor '{}'", op->name()));
      element.second.setActorHandle(caf::actor_cast<caf::actor>(actorHandle));
	  } else {
      if(op->getType() == "Collate"){
        legacyCollateOperator_ = std::static_pointer_cast<Collate>(op);
      }
	    auto actorHandle = operatorManager_.lock()->getActorSystem()->spawn<normal::core::OperatorActor>(op);
	    if (!actorHandle)
        throw std::runtime_error(fmt::format("Failed to spawn operator actor '{}'", op->name()));
      element.second.setActorHandle(caf::actor_cast<caf::actor>(actorHandle));
	  }
	}
  }
}

void graph::OperatorGraph::write_graph(const std::string &file) {

  auto gvc = gvContext();

  auto graph = agopen(const_cast<char *>(std::string("Execution Plan").c_str()), Agstrictdirected, nullptr);

  // Init attributes
  agattr(graph, AGNODE, const_cast<char *>("fixedsize"), const_cast<char *>("false"));
  agattr(graph, AGNODE, const_cast<char *>("shape"), const_cast<char *>("ellipse"));
  agattr(graph, AGNODE, const_cast<char *>("label"), const_cast<char *>("<not set>"));
  agattr(graph, AGNODE, const_cast<char *>("fontname"), const_cast<char *>("Arial"));
  agattr(graph, AGNODE, const_cast<char *>("fontsize"), const_cast<char *>("8"));

  // Add all the nodes
  for (const auto &op: this->operatorDirectory_) {
    std::string nodeName = op.second.getDef()->name();
    auto node = agnode(graph, const_cast<char *>(nodeName.c_str()), true);

    agset(node, const_cast<char *>("shape"), const_cast<char *>("plaintext"));

    std::string nodeLabel = "<table border='1' cellborder='0' cellpadding='5'>"
                "<tr><td align='left'><b>" + op.second.getDef()->getType() + "</b></td></tr>"
                                              "<tr><td align='left'>"
      + op.second.getDef()->name() + "</td></tr>"
                    "</table>";
    char *htmlNodeLabel = agstrdup_html(graph, const_cast<char *>(nodeLabel.c_str()));
    agset(node, const_cast<char *>("label"), htmlNodeLabel);
    agstrfree(graph, htmlNodeLabel);
  }

  // Add all the edges
  for (const auto &op: this->operatorDirectory_) {
    auto opNode = agfindnode(graph, (char *)(op.second.getDef()->name().c_str()));
    for (const auto &c: op.second.getDef()->consumers()) {
      auto consumerOpNode = agfindnode(graph, (char *)(c.second.c_str()));
      agedge(graph, opNode, consumerOpNode, const_cast<char *>(std::string("Edge").c_str()), true);
    }
  }

  const std::filesystem::path &path = std::filesystem::path(file);
  if (!std::filesystem::exists(path.parent_path())) {
	  throw std::runtime_error("Could not open file '" + file + "' for writing. Parent directory does not exist");
  } else {
    FILE *outFile = fopen(file.c_str(), "w");
    if (outFile == nullptr) {
      throw std::runtime_error("Could not open file '" + file + "' for writing. Errno: " + std::to_string(errno));
    }

    gvLayout(gvc, graph, "dot");
    gvRender(gvc, graph, "svg", outFile);

    fclose(outFile);

    gvFreeLayout(gvc, graph);
    agclose(graph);
    gvFreeContext(gvc);
  }
}

tl::expected<long, std::string> graph::OperatorGraph::getElapsedTime() {

  if (startTime_.time_since_epoch().count() == 0)
	  return tl::unexpected(std::string("Execution time unavailable, query has not been started"));
  if (stopTime_.time_since_epoch().count() == 0)
	  return tl::unexpected(std::string("Execution time unavailable, query has not been stopped"));

  return std::chrono::duration_cast<std::chrono::nanoseconds>(stopTime_ - startTime_).count();
}

std::shared_ptr<Operator> graph::OperatorGraph::getOperator(const std::string &name) {
  return operatorDirectory_.get(name).value().getDef();
}

std::string graph::OperatorGraph::showMetrics(bool showOpTimes, bool showScanMetrics) {
  std::stringstream ss;
  if (!showOpTimes && !showScanMetrics) {
    return ss.str();
  }

  ss << std::endl;
  ss << "Metrics |" << std::endl << std::endl;
  long totalProcessingTime = 0;
  for (auto &entry : operatorDirectory_) {
	(*rootActor_)->request(entry.second.getActorHandle(), caf::infinite, GetProcessingTimeAtom::value).receive(
		[&](long processingTime) {
		  totalProcessingTime += processingTime;
		},
		[&](const caf::error&  error){
		  throw std::runtime_error(to_string(error));
		});
  }

  if (showOpTimes) {
    auto totalExecutionTime = getElapsedTime().value();
    std::stringstream formattedExecutionTime;
    formattedExecutionTime << totalExecutionTime << " \u33B1" << " (" << ((double) totalExecutionTime / 1000000000.0)
                           << " secs)";
    ss << std::left << std::setw(60) << "Total Execution Time ";
    ss << std::left << std::setw(60) << formattedExecutionTime.str();
    ss << std::endl;
    ss << std::endl;

    ss << std::left << std::setw(120) << "Operator Execution Times" << std::endl;
    ss << std::setfill(' ');

    ss << std::left << std::setw(120) << std::setfill('-') << "" << std::endl;
    ss << std::setfill(' ');

    ss << std::left << std::setw(60) << "Operator";
    ss << std::left << std::setw(40) << "Execution Time";
    ss << std::left << std::setw(20) << "% Total Time";
    ss << std::endl;

    ss << std::left << std::setw(120) << std::setfill('-') << "" << std::endl;
    ss << std::setfill(' ');

    std::map<std::string, size_t> opTypeToRuntime;

    for (auto &entry : operatorDirectory_) {
      auto operatorName = entry.first;

      long processingTime;
      (*rootActor_)->request(entry.second.getActorHandle(), caf::infinite, GetProcessingTimeAtom::value).receive(
              [&](long time) {
                processingTime = time;
              },
              [&](const caf::error &error) {
                throw std::runtime_error(to_string(error));
              });

      auto op = entry.second.getDef();
      std::string type = op->getType();
      if (opTypeToRuntime.find(type) == opTypeToRuntime.end()) {
        opTypeToRuntime.emplace(type, processingTime);
      } else {
        opTypeToRuntime[type] = opTypeToRuntime[type] + processingTime;
      }

      auto processingFraction = (double) processingTime / (double) totalProcessingTime;
      std::stringstream formattedProcessingTime;
      formattedProcessingTime << processingTime << " \u33B1" << " (" << ((double) processingTime / 1000000000.0)
                              << " secs)";
      std::stringstream formattedProcessingPercentage;
      formattedProcessingPercentage << (processingFraction * 100.0);
      ss << std::left << std::setw(60) << operatorName;
      ss << std::left << std::setw(40) << formattedProcessingTime.str();
      ss << std::left << std::setw(20) << formattedProcessingPercentage.str();
      ss << std::endl;
    }

    ss << std::left << std::setw(120) << std::setfill('-') << "" << std::endl;
    ss << std::setfill(' ');
    ss << std::endl;

    for (auto const &opTime : opTypeToRuntime) {
      std::stringstream formattedOpTime;
      formattedOpTime << ((double) opTime.second / 1000000000.0) << " secs";
      ss << std::left << std::setw(60) << opTime.first;
      ss << std::left << std::setw(40) << formattedOpTime.str();
      ss << std::left << std::setw(20) << ((double) opTime.second / (double) totalProcessingTime) * 100.0;
      ss << std::endl;
    }

    ss << std::left << std::setw(120) << std::setfill('-') << "" << std::endl;
    ss << std::setfill(' ');

    std::stringstream formattedProcessingTime;
    formattedProcessingTime << totalProcessingTime << " \u33B1" << " (" << ((double) totalProcessingTime / 1000000000.0)
                            << " secs)";
    ss << std::left << std::setw(60) << "Total ";
    ss << std::left << std::setw(40) << formattedProcessingTime.str();
    ss << std::left << std::setw(20) << "100.0";
    ss << std::endl;
    ss << std::endl;
  }

  if (showScanMetrics) {
    S3SelectScanStats s3SelectScanStats = getAggregateS3SelectScanStats();

    std::stringstream formattedProcessedBytes;
    formattedProcessedBytes << s3SelectScanStats.processedBytes << " B" << " ("
                            << ((double) s3SelectScanStats.processedBytes / 1024.0 / 1024.0 / 1024.0) << " GB)";
    std::stringstream formattedReturnedBytes;
    formattedReturnedBytes << s3SelectScanStats.returnedBytes << " B" << " ("
                           << ((double) s3SelectScanStats.returnedBytes / 1024.0 / 1024.0 / 1024.0) << " GB)";
    std::stringstream formattedArrowConvertedBytes;
    formattedArrowConvertedBytes << s3SelectScanStats.outputBytes << " B" << " ("
                                 << ((double) s3SelectScanStats.outputBytes / 1024.0 / 1024.0 / 1024.0) << " GB)";

    std::stringstream formattedConversionOutputRate;
    if (s3SelectScanStats.getConvertTimeNS + s3SelectScanStats.selectConvertTimeNS > 0) {
      formattedConversionOutputRate << ((double) s3SelectScanStats.outputBytes / 1024.0 / 1024.0) /
                                       ((double) (s3SelectScanStats.getConvertTimeNS +
                                                  s3SelectScanStats.selectConvertTimeNS) / 1.0e9) << " MB/s/req";
    } else {
      formattedConversionOutputRate << "NA";
    }

    std::stringstream formattedStorageFormatToArrowSizeX;
    if (s3SelectScanStats.outputBytes > 0) {
      formattedStorageFormatToArrowSizeX
              << (double) s3SelectScanStats.returnedBytes / (double) s3SelectScanStats.outputBytes << "x";
    } else {
      formattedStorageFormatToArrowSizeX << "NA";
    }
    ss << std::left << std::setw(60) << "Processed Bytes";
    ss << std::left << std::setw(60) << formattedProcessedBytes.str();
    ss << std::endl;
    ss << std::left << std::setw(60) << "Returned Bytes";
    ss << std::left << std::setw(60) << formattedReturnedBytes.str();
    ss << std::endl;
    ss << std::left << std::setw(60) << "Arrow Converted Bytes";
    ss << std::left << std::setw(60) << formattedArrowConvertedBytes.str();
    ss << std::endl;
    ss << std::left << std::setw(60) << "Conversion Output Rate";
    ss << std::left << std::setw(60) << formattedConversionOutputRate.str();
    ss << std::endl;
    ss << std::left << std::setw(60) << "Storage/Compute Data Ratio";
    ss << std::left << std::setw(60) << formattedStorageFormatToArrowSizeX.str();
    ss << std::endl;

    std::stringstream formattedGetTransferRate;
    std::stringstream formattedGetConvertRate;
    std::stringstream formattedGetTransferConvertRate;
    if (s3SelectScanStats.getTransferTimeNS > 0 && s3SelectScanStats.getConvertTimeNS > 0) {
      formattedGetTransferRate << ((double) s3SelectScanStats.returnedBytes / 1024.0 / 1024.0) /
                                  ((double) s3SelectScanStats.getTransferTimeNS / 1.0e9) << " MB/s/req";
      formattedGetConvertRate << ((double) s3SelectScanStats.returnedBytes / 1024.0 / 1024.0) /
                                 ((double) s3SelectScanStats.getConvertTimeNS / 1.0e9) << " MB/s/req";
      formattedGetTransferConvertRate << ((double) s3SelectScanStats.returnedBytes / 1024.0 / 1024.0) /
                                         (((double) s3SelectScanStats.getTransferTimeNS +
                                           s3SelectScanStats.getConvertTimeNS) / 1.0e9) << " MB/s/req";
    } else {
      formattedGetTransferRate << "NA";
      formattedGetConvertRate << "NA";
      formattedGetTransferConvertRate << "NA";
    }
    // Caf actor framework seems to converge #workers -> # cores for non detached workers, and each worker runs to
    // completion. This should approximate to per core rates rather than just per request, as one request maps to a core
    ss << std::left << std::setw(60) << "S3 GET Data Transfer Rate";
    ss << std::left << std::setw(60) << formattedGetTransferRate.str();
    ss << std::endl;
    ss << std::left << std::setw(60) << "S3 GET Data Convert rate";
    ss << std::left << std::setw(60) << formattedGetConvertRate.str();
    ss << std::endl;
    ss << std::left << std::setw(60) << "S3 GET Data Transfer + Convert rate";
    ss << std::left << std::setw(60) << formattedGetTransferConvertRate.str();
    ss << std::endl;

    std::stringstream formattedSelectTransferRate;
    std::stringstream formattedSelectConvertRate;
    std::stringstream formattedSelectTransferConvertRate;
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
    std::stringstream formattedS3SelectSelectivityPercent;
    if (s3SelectScanStats.returnedBytes > 0 && s3SelectScanStats.processedBytes &&
        s3SelectScanStats.selectTransferTimeNS > 0 && s3SelectScanStats.getConvertTimeNS == 0) {
      formattedS3SelectSelectivityPercent << (double) s3SelectScanStats.returnedBytes /
                                             (double) s3SelectScanStats.processedBytes * 100 << "%";
    } else {
      formattedS3SelectSelectivityPercent << "NA";
    }
    ss << std::left << std::setw(60) << "Appx S3 Select Data Transfer Rate";
    ss << std::left << std::setw(60) << formattedSelectTransferRate.str();
    ss << std::endl;
    ss << std::left << std::setw(60) << "S3 Select Data Convert rate";
    ss << std::left << std::setw(60) << formattedSelectConvertRate.str();
    ss << std::endl;
    ss << std::left << std::setw(60) << "S3 Select Data Transfer + Convert rate";
    ss << std::left << std::setw(60) << formattedSelectTransferConvertRate.str();
    ss << std::endl;
    ss << std::left << std::setw(60) << "S3 Selectivity";
    ss << std::left << std::setw(60) << formattedS3SelectSelectivityPercent.str();
    ss << std::endl;

    auto filterTimeNSInputOutputBytes = getFilterTimeNSInputOutputBytes();
    size_t filterTimeNS = std::get<0>(filterTimeNSInputOutputBytes);
    size_t filterInputBytes = std::get<1>(filterTimeNSInputOutputBytes);
    size_t filterOutputBytes = std::get<2>(filterTimeNSInputOutputBytes);
    std::stringstream formattedLocalFilterRateGBs;
    std::stringstream formattedLocalFilterGB;
    std::stringstream formattedLocalFilterSelectivity;
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
    ss << std::left << std::setw(60) << "Local filter rate";
    ss << std::left << std::setw(60) << formattedLocalFilterRateGBs.str();
    ss << std::endl;
    ss << std::left << std::setw(60) << "Local filter bytes";
    ss << std::left << std::setw(60) << formattedLocalFilterGB.str();
    ss << std::endl;
    ss << std::left << std::setw(60) << "Local filter selectivity (bytes)";
    ss << std::left << std::setw(60) << formattedLocalFilterSelectivity.str();
    ss << std::endl;
    ss << std::endl;
  }

  return ss.str();
}

const long &graph::OperatorGraph::getId() const {
  return id_;
}

S3SelectScanStats graph::OperatorGraph::getAggregateS3SelectScanStats() {
  S3SelectScanStats aggregateS3SelectScanStats = {0, 0, 0, 0 , 0, 0 ,0, 0}; // initialize all fields to 0
  for (const auto &entry: operatorDirectory_) {
    if (typeid(*entry.second.getDef()) == typeid(S3Select) ||
        typeid(*entry.second.getDef()) == typeid(S3Get)) {
//	  (*rootActor_)->request(entry.second.getActorHandle(), caf::infinite, GetMetricsAtom::value).receive(
//	  	[&](std::pair<size_t, size_t> metrics) {
//		  processedBytes += metrics.first;
//		  returnedBytes += metrics.second;
//		},
//		[&](const caf::error&  error){
//	  	  throw std::runtime_error(to_string(error));
//	  	});

	  // FIXME: Really need to get metrics with a message as above (just interrogating the operator directly is unsafe).
      auto s3SelectScanOp = std::static_pointer_cast<S3SelectScan>(entry.second.getDef());
      S3SelectScanStats currentS3SelectScanStats = s3SelectScanOp->getS3SelectScanStats();
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

std::tuple<size_t, size_t, size_t> graph::OperatorGraph::getFilterTimeNSInputOutputBytes() {
  size_t timeNS = 0;
  size_t inputBytes = 0;
  size_t outputBytes = 0;
  for (const auto &entry: operatorDirectory_) {
    if (typeid(*entry.second.getDef()) == typeid(normal::pushdown::filter::Filter)) {
//	  (*rootActor_)->request(entry.second.getActorHandle(), caf::infinite, GetMetricsAtom::value).receive(
//	  	[&](std::pair<size_t, size_t> metrics) {
//		  processedBytes += metrics.first;
//		  returnedBytes += metrics.second;
//		},
//		[&](const caf::error&  error){
//	  	  throw std::runtime_error(to_string(error));
//	  	});

      // FIXME: Really need to get metrics with a message as above (just interrogating the operator directly is unsafe).
      auto filterOp = std::static_pointer_cast<normal::pushdown::filter::Filter>(entry.second.getDef());
      timeNS += filterOp->getFilterTimeNS();
      inputBytes += filterOp->getFilterInputBytes();
      outputBytes += filterOp->getFilterOutputBytes();
	  }
  }
  return std::tuple<size_t, size_t, size_t>(timeNS, inputBytes, outputBytes);
}

void graph::OperatorGraph::close() {
  if(rootActor_ != nullptr){
	for (const auto &element: operatorDirectory_) {
	  (*rootActor_)->send_exit(element.second.getActorHandle(), caf::exit_reason::user_shutdown);
	}
	rootActor_.reset();
  }

  legacyCollateOperator_.reset();
}

graph::OperatorGraph::~OperatorGraph() {
  close();
}

tl::expected<std::shared_ptr<TupleSet2>, std::string> graph::OperatorGraph::execute() {

  boot();
  start();
  join();

  auto tuples = legacyCollateOperator_->tuples();
  return TupleSet2::create(tuples);
}

std::shared_ptr<TupleSet> graph::OperatorGraph::getQueryResult() const {
  return legacyCollateOperator_->tuples();
}
