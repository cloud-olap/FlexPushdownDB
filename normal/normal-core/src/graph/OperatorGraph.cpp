//
// Created by matt on 7/7/20.
//

#include "normal/core/graph/OperatorGraph.h"

#include <cassert>

#include <caf/all.hpp>
#include <experimental/filesystem>
#include <utility>
#include <graphviz/gvc.h>

#include <normal/core/Actors.h>
#include <normal/core/OperatorDirectoryEntry.h>
#include <normal/core/Globals.h>
#include <normal/pushdown/file/FileScan2.h>
#include <normal/pushdown/Collate.h>
#include <normal/pushdown/collate/Collate2.h>
#include <normal/pushdown/s3/S3SelectScan.h>
#include <normal/pushdown/s3/S3SelectScan2.h>
#include <normal/pushdown/s3/S3Select.h>
#include <normal/pushdown/s3/S3Get.h>
#include <normal/core/message/ConnectMessage.h>


using namespace normal::core::graph;
using namespace normal::core;
using namespace std::experimental;
using namespace normal::pushdown;

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


//   Connect the actors
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

//        SPDLOG_DEBUG(fmt::format("Operator directory:\n{}", this->operatorDirectory_.showString()));
//        SPDLOG_DEBUG(fmt::format("All operators complete: {}", allComplete));
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
	  if (op->getType() == "S3Select") {
	    auto actorHandle = operatorManager_.lock()->getActorSystem()->spawn<normal::core::OperatorActor, detached>(op);
	    if (!actorHandle)
      throw std::runtime_error(fmt::format("Failed to spawn operator actor '{}'", op->name()));
      element.second.setActorHandle(caf::actor_cast<caf::actor>(actorHandle));
	  } else {
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

  auto graph = agopen(const_cast<char *>(std::string("Execution Plan").c_str()), Agstrictdirected, 0);

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

  const std::experimental::filesystem::path &path = std::experimental::filesystem::path(file);
  if (!std::experimental::filesystem::exists(path.parent_path())) {
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

std::string graph::OperatorGraph::showMetrics() {

  std::stringstream ss;

  ss << std::endl;

  long totalProcessingTime = 0;
  for (auto &entry : operatorDirectory_) {
//	auto processingTime = entry.second.getOperatorContext().lock()->operatorActor()->getProcessingTime();
	(*rootActor_)->request(entry.second.getActorHandle(), caf::infinite, GetProcessingTimeAtom::value).receive(
		[&](long processingTime) {
		  totalProcessingTime += processingTime;
		},
		[&](const caf::error&  error){
		  throw std::runtime_error(to_string(error));
		});

//	auto timeSpan = operatorManager_.lock()->processingTimeSpans_.find(entry.second.getActorHandle().id());
//	auto processingTime = std::chrono::duration_cast<std::chrono::nanoseconds>(timeSpan->second.second - timeSpan->second.first).count();
//	totalProcessingTime += processingTime;
  }

  auto totalExecutionTime = getElapsedTime().value();
  std::stringstream formattedExecutionTime;
  formattedExecutionTime << totalExecutionTime << " \u33B1" << " (" << ((double)totalExecutionTime / 1000000000.0)
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

  for (auto &entry : operatorDirectory_) {
	auto operatorName = entry.first;
//	auto processingTime = entry.second.getOperatorContext().lock()->operatorActor()->getProcessingTime();
//	auto timeSpan = operatorManager_.lock()->processingTimeSpans_.find(entry.second.getActorHandle().id());
	long processingTime;
	(*rootActor_)->request(entry.second.getActorHandle(), caf::infinite, GetProcessingTimeAtom::value).receive(
		[&](long time) {
		  processingTime = time;
		},
		[&](const caf::error&  error){
		  throw std::runtime_error(to_string(error));
		});
//	auto processingTime = std::chrono::duration_cast<std::chrono::nanoseconds>(timeSpan->second.second - timeSpan->second.first).count();
	auto processingFraction = (double)processingTime / (double)totalProcessingTime;
	std::stringstream formattedProcessingTime;
	formattedProcessingTime << processingTime << " \u33B1" << " (" << ((double)processingTime / 1000000000.0)
							<< " secs)";
	std::stringstream formattedProcessingPercentage;
	formattedProcessingPercentage << (processingFraction * 100.0);
//	ss << std::left << std::setw(60) << operatorName;
//	ss << std::left << std::setw(40) << formattedProcessingTime.str();
//	ss << std::left << std::setw(20) << formattedProcessingPercentage.str();
//	ss << std::endl;
  }

  ss << std::left << std::setw(120) << std::setfill('-') << "" << std::endl;
  ss << std::setfill(' ');

  std::stringstream formattedProcessingTime;
  formattedProcessingTime << totalProcessingTime << " \u33B1" << " (" << ((double)totalProcessingTime / 1000000000.0)
						  << " secs)";
  ss << std::left << std::setw(60) << "Total ";
  ss << std::left << std::setw(40) << formattedProcessingTime.str();
  ss << std::left << std::setw(20) << "100.0";
  ss << std::endl;
  ss << std::endl;

  auto bytesTransferred = getBytesTransferred();
  std::stringstream formattedProcessedBytes;
  formattedProcessedBytes << bytesTransferred.first << " B" << " ("
                          << ((double)bytesTransferred.first / 1024.0 / 1024.0 / 1024.0) << " GB)";
  std::stringstream formattedReturnedBytes;
  formattedReturnedBytes << bytesTransferred.second << " B" << " ("
                         << ((double)bytesTransferred.second / 1024.0 / 1024.0 / 1024.0) << " GB)";
  ss << std::left << std::setw(60) << "Processed Bytes";
  ss << std::left << std::setw(60) << formattedProcessedBytes.str();
  ss << std::endl;
  ss << std::left << std::setw(60) << "Returned Bytes";
  ss << std::left << std::setw(60) << formattedReturnedBytes.str();
  ss << std::endl;

  auto getTransferConvertTimesNS = getGetTransferConvertTimesNS();
  std::stringstream formattedGetTransferRate;
  std::stringstream formattedGetConvertRate;
  std::stringstream formattedGetTransferConvertRate;
  if (getTransferConvertTimesNS.first > 0 && getTransferConvertTimesNS.second > 0) {
    formattedGetTransferRate << ((double) bytesTransferred.second / 1024.0 / 1024.0) /
                             ((double) getTransferConvertTimesNS.first / 1.0e9) << " MB/s/req";
    formattedGetConvertRate << ((double) bytesTransferred.second / 1024.0 / 1024.0) /
                            ((double) getTransferConvertTimesNS.second / 1.0e9) << " MB/s/req";
    formattedGetTransferConvertRate << ((double) bytesTransferred.second / 1024.0 / 1024.0) /
                            (((double) getTransferConvertTimesNS.first + getTransferConvertTimesNS.second) / 1.0e9) << " MB/s/req";
  } else {
    formattedGetTransferRate << "NA";
    formattedGetConvertRate << "NA";
    formattedGetTransferConvertRate << "NA";
  }
  // Caf actor framework seems to converge #workers -> # cores, and each worker runs to completion
  // so this should approximate to per core rates rather than just per request, as one request maps to a core
  ss << std::left << std::setw(60) << "S3 GET Data Transfer Rate";
  ss << std::left << std::setw(60) << formattedGetTransferRate.str();
  ss << std::endl;
  ss << std::left << std::setw(60) << "S3 GET Data Convert rate";
  ss << std::left << std::setw(60) << formattedGetConvertRate.str();
  ss << std::endl;
  ss << std::left << std::setw(60) << "S3 GET Data Transfer and Convert rate";
  ss << std::left << std::setw(60) << formattedGetTransferConvertRate.str();
  ss << std::endl;

  auto selectTransferConvertTimesNS = getSelectTransferConvertTimesNS();
  std::stringstream formattedSelectTransferRate;
  std::stringstream formattedSelectConvertRate;
  std::stringstream formattedSelectTransferConvertRate;
  if (selectTransferConvertTimesNS.first > 0 && selectTransferConvertTimesNS.second > 0) {
    formattedSelectTransferRate << ((double)bytesTransferred.second / 1024.0 / 1024.0) /
          ((double)(selectTransferConvertTimesNS.first - selectTransferConvertTimesNS.second)  / 1.0e9) << " MB/s/req";
    formattedSelectConvertRate << ((double)bytesTransferred.second / 1024.0 / 1024.0) /
          ((double)selectTransferConvertTimesNS.second / 1.0e9) << " MB/s/req";
    formattedSelectTransferConvertRate << ((double)bytesTransferred.second / 1024.0 / 1024.0) /
          ((double)(selectTransferConvertTimesNS.first)  / 1.0e9) << " MB/s/req";
  } else {
    formattedSelectTransferRate << "NA";
    formattedSelectConvertRate << "NA";
    formattedSelectTransferConvertRate << "NA";
  }
  // FIXME: This only works if the query is entires pushdown, as the bytes transferred is grouped together for
  //        select and get requests, so they are not differentiated
  std::stringstream formattedS3SelectSelectivity;
  if (bytesTransferred.second > 0 && selectTransferConvertTimesNS.first > 0 && getTransferConvertTimesNS.second == 0) {
    formattedS3SelectSelectivity << (double) bytesTransferred.second / (double) bytesTransferred.first;
  } else {
    formattedS3SelectSelectivity << "NA";
  }
  // Caf actor framework seems to converge #workers -> # cores, and each worker runs to completion
  // so this should approximate to per core rates rather than just per request, as one request maps to a core
  ss << std::left << std::setw(60) << "Appx S3 Select Data Transfer Rate";
  ss << std::left << std::setw(60) << formattedSelectTransferRate.str();
  ss << std::endl;
  ss << std::left << std::setw(60) << "S3 Select Data Convert rate";
  ss << std::left << std::setw(60) << formattedSelectConvertRate.str();
  ss << std::endl;
  ss << std::left << std::setw(60) << "S3 Select Data Transfer + Convert rate";
  ss << std::left << std::setw(60) << formattedSelectTransferConvertRate.str();
  ss << std::endl;
  ss << std::left << std::setw(60) << "% Data S3 Selected";
  ss << std::left << std::setw(60) << formattedS3SelectSelectivity.str();
  ss << std::endl;
  ss << std::endl;

  return ss.str();
}

const long &graph::OperatorGraph::getId() const {
  return id_;
}

std::pair<size_t, size_t> graph::OperatorGraph::getBytesTransferred() {
  size_t processedBytes = 0;
  size_t returnedBytes = 0;
  for (const auto &entry: operatorDirectory_) {
    if (typeid(*entry.second.getDef()) == typeid(normal::pushdown::S3Select) ||
        typeid(*entry.second.getDef()) == typeid(normal::pushdown::S3Get)) {
//	  (*rootActor_)->request(entry.second.getActorHandle(), caf::infinite, GetMetricsAtom::value).receive(
//	  	[&](std::pair<size_t, size_t> metrics) {
//		  processedBytes += metrics.first;
//		  returnedBytes += metrics.second;
//		},
//		[&](const caf::error&  error){
//	  	  throw std::runtime_error(to_string(error));
//	  	});

	  // FIXME: Really need to get metrics with a message as above (just interrogating the operator directly is unsafe).
      auto s3ScanOp = std::static_pointer_cast<normal::pushdown::S3SelectScan>(entry.second.getDef());
      processedBytes += s3ScanOp->getProcessedBytes();
      returnedBytes += s3ScanOp->getReturnedBytes();
	}
  }
  return std::pair<size_t, size_t>(processedBytes, returnedBytes);
}

size_t graph::OperatorGraph::getNumRequests() {
  size_t numRequests = 0;
  for (const auto &entry: operatorDirectory_) {
    if (typeid(*entry.second.getDef()) == typeid(normal::pushdown::S3Select) ||
        typeid(*entry.second.getDef()) == typeid(normal::pushdown::S3Get)) {
	  // FIXME: Really need to get metrics with a message as above (just interrogating the operator directly is unsafe).
      auto s3ScanOp = std::static_pointer_cast<normal::pushdown::S3SelectScan>(entry.second.getDef());
      numRequests += s3ScanOp->getNumRequests();
    }
  }
  return numRequests;
}

std::pair<size_t, size_t> graph::OperatorGraph::getGetTransferConvertTimesNS() {
  size_t getTransferTimeNS = 0;
  size_t getConvertTimeNS = 0;
  for (const auto &entry: operatorDirectory_) {
    if (typeid(*entry.second.getDef()) == typeid(normal::pushdown::S3Select) ||
        typeid(*entry.second.getDef()) == typeid(normal::pushdown::S3Get)) {
//	  (*rootActor_)->request(entry.second.getActorHandle(), caf::infinite, GetMetricsAtom::value).receive(
//	  	[&](std::pair<size_t, size_t> metrics) {
//		  processedBytes += metrics.first;
//		  returnedBytes += metrics.second;
//		},
//		[&](const caf::error&  error){
//	  	  throw std::runtime_error(to_string(error));
//	  	});

	  // FIXME: Really need to get metrics with a message as above (just interrogating the operator directly is unsafe).
      auto s3ScanOp = std::static_pointer_cast<normal::pushdown::S3SelectScan>(entry.second.getDef());
      getTransferTimeNS += s3ScanOp->getGetTransferTimeNS();
      getConvertTimeNS += s3ScanOp->getGetConvertTimeNS();
	}
  }
  return std::pair<size_t, size_t>(getTransferTimeNS, getConvertTimeNS);
}

std::pair<size_t, size_t> graph::OperatorGraph::getSelectTransferConvertTimesNS() {
  size_t selectTransferTimeNS = 0;
  size_t selectConvertTimeNS = 0;
  for (const auto &entry: operatorDirectory_) {
    if (typeid(*entry.second.getDef()) == typeid(normal::pushdown::S3Select) ||
        typeid(*entry.second.getDef()) == typeid(normal::pushdown::S3Get)) {
//	  (*rootActor_)->request(entry.second.getActorHandle(), caf::infinite, GetMetricsAtom::value).receive(
//	  	[&](std::pair<size_t, size_t> metrics) {
//		  processedBytes += metrics.first;
//		  returnedBytes += metrics.second;
//		},
//		[&](const caf::error&  error){
//	  	  throw std::runtime_error(to_string(error));
//	  	});

	  // FIXME: Really need to get metrics with a message as above (just interrogating the operator directly is unsafe).
      auto s3ScanOp = std::static_pointer_cast<normal::pushdown::S3SelectScan>(entry.second.getDef());
      selectTransferTimeNS += s3ScanOp->getSelectTransferTimeNS();
      selectConvertTimeNS += s3ScanOp->getSelectConvertTimeNS();
	}
  }
  return std::pair<size_t, size_t>(selectTransferTimeNS, selectConvertTimeNS);
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
