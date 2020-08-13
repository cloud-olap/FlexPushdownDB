//
// Created by matt on 26/6/20.
//

#include "normal/ssb/query1_1/LocalFileSystemQueries.h"

#include <normal/ssb/Globals.h>
#include <normal/ssb/query1_1/Operators.h>
#include <normal/core/Normal.h>
#include <normal/ssb/common/Operators.h>
#include <normal/ssb/SSBSchema.h>

using namespace normal::ssb::query1_1;
using namespace normal::pushdown::aggregate;
using namespace normal::core::type;
using namespace normal::expression::gandiva;

std::shared_ptr<OperatorGraph>
LocalFileSystemQueries::dateScan(const std::string &dataDir, int numConcurrentUnits, const std::shared_ptr<OperatorManager>& mgr) {

  auto g = OperatorGraph::make(mgr);

  auto dateCacheLoads = Operators::makeDateFileCacheLoadOperators(dataDir, numConcurrentUnits, g);
  auto dateScans = Operators::makeDateFileScanOperators(dataDir, numConcurrentUnits, g);
  auto dateMerges = Operators::makeDateMergeOperators(numConcurrentUnits, g);
  auto collate = Operators::makeCollateOperator(g);

  // Wire up
  for (int u = 0; u < numConcurrentUnits; ++u) {
	dateCacheLoads[u]->setHitOperator(dateMerges[u]);
	dateMerges[u]->consume(dateCacheLoads[u]);
  }

  for (int u = 0; u < numConcurrentUnits; ++u) {
	dateCacheLoads[u]->setMissOperator(dateScans[u]);
	dateScans[u]->consume(dateCacheLoads[u]);
  }

  for (int u = 0; u < numConcurrentUnits; ++u) {
	dateScans[u]->produce(dateMerges[u]);
	dateMerges[u]->consume(dateScans[u]);
  }

  for (int u = 0; u < numConcurrentUnits; ++u) {
	dateMerges[u]->produce(collate);
	collate->consume(dateMerges[u]);
  }

  for (int u = 0; u < numConcurrentUnits; ++u)
	g->put(dateCacheLoads[u]);
  for (int u = 0; u < numConcurrentUnits; ++u)
	g->put(dateMerges[u]);
  for (int u = 0; u < numConcurrentUnits; ++u)
	g->put(dateScans[u]);
  g->put(collate);

  return g;
}

std::shared_ptr<OperatorGraph>
LocalFileSystemQueries::dateScan(const std::string &dataDir, FileType fileType, int numConcurrentUnits, const std::shared_ptr<Normal> &n) {

  auto g = n->createQuery();

  auto dateCacheLoads = common::Operators::makeFileCacheLoadOperators("date", fileType == FileType::CSV ? "date.tbl" : "parquet/date.snappy.parquet", SSBSchema::DateFields, dataDir, numConcurrentUnits, g);
  auto dateScans = common::Operators::makeFileScanOperators("date", fileType == FileType::CSV ? "date.tbl" : "parquet/date.snappy.parquet", fileType, SSBSchema::DateFields, dataDir, numConcurrentUnits, g);
  auto dateMerges = common::Operators::makeMergeOperators("date", numConcurrentUnits, g);
  auto collate = common::Operators::makeCollateOperator(g);

  common::Operators::connectHitsToEach(dateCacheLoads, dateMerges);
  common::Operators::connectMissesToEach(dateCacheLoads, dateScans);
  common::Operators::connectToEach(dateScans, dateMerges);

  common::Operators::connectToOne(dateMerges, collate);

  return g;
}

std::shared_ptr<OperatorGraph>
LocalFileSystemQueries::dateFilter(const std::string &dataDir, short year, int numConcurrentUnits,
								   const std::shared_ptr<OperatorManager> &mgr) {

  auto g = OperatorGraph::make(mgr);

  auto dateCacheLoads = Operators::makeDateFileCacheLoadOperators(dataDir, numConcurrentUnits, g);
  auto dateScans = Operators::makeDateFileScanOperators(dataDir, numConcurrentUnits, g);
  auto dateMerges = Operators::makeDateMergeOperators(numConcurrentUnits, g);
  auto dateFilters = Operators::makeDateFilterOperators(year, true, numConcurrentUnits, g);
  auto collate = Operators::makeCollateOperator(g);

  // Wire up
  for (int u = 0; u < numConcurrentUnits; ++u) {
	dateCacheLoads[u]->setHitOperator(dateMerges[u]);
	dateMerges[u]->consume(dateCacheLoads[u]);
  }

  for (int u = 0; u < numConcurrentUnits; ++u) {
	dateCacheLoads[u]->setMissOperator(dateScans[u]);
	dateScans[u]->consume(dateCacheLoads[u]);
  }

  for (int u = 0; u < numConcurrentUnits; ++u) {
	dateScans[u]->produce(dateMerges[u]);
	dateMerges[u]->consume(dateScans[u]);
  }

  for (int u = 0; u < numConcurrentUnits; ++u) {
	dateMerges[u]->produce(dateFilters[u]);
	dateFilters[u]->consume(dateMerges[u]);
  }

  for (int u = 0; u < numConcurrentUnits; ++u) {
	dateFilters[u]->produce(collate);
	collate->consume(dateFilters[u]);
  }

  for (int u = 0; u < numConcurrentUnits; ++u)
	g->put(dateCacheLoads[u]);
  for (int u = 0; u < numConcurrentUnits; ++u)
	g->put(dateMerges[u]);
  for (int u = 0; u < numConcurrentUnits; ++u)
	g->put(dateScans[u]);
  for (int u = 0; u < numConcurrentUnits; ++u)
	g->put(dateFilters[u]);
  g->put(collate);

  return g;
}

std::shared_ptr<OperatorGraph>
LocalFileSystemQueries::lineOrderScan(const std::string &dataDir,
									  int numConcurrentUnits,
									  const std::shared_ptr<OperatorManager> &mgr) {

  auto g = OperatorGraph::make(mgr);

  auto lineOrderCacheLoads = Operators::makeLineOrderFileCacheLoadOperators(dataDir, numConcurrentUnits, g);
  auto lineOrderScans = Operators::makeLineOrderFileScanOperators(dataDir, numConcurrentUnits, g);
  auto lineOrderMerges = Operators::makeLineOrderMergeOperators(numConcurrentUnits, g);
  auto collate = Operators::makeCollateOperator(g);

	// Wire up
  for (int u = 0; u < numConcurrentUnits; ++u) {
	lineOrderCacheLoads[u]->setHitOperator(lineOrderMerges[u]);
	lineOrderMerges[u]->consume(lineOrderCacheLoads[u]);
  }

  for (int u = 0; u < numConcurrentUnits; ++u) {
	lineOrderCacheLoads[u]->setMissOperator(lineOrderScans[u]);
	lineOrderScans[u]->consume(lineOrderCacheLoads[u]);
  }

  for (int u = 0; u < numConcurrentUnits; ++u) {
	lineOrderScans[u]->produce(lineOrderMerges[u]);
	lineOrderMerges[u]->consume(lineOrderScans[u]);
  }

  for (int u = 0; u < numConcurrentUnits; ++u) {
	lineOrderMerges[u]->produce(collate);
	collate->consume(lineOrderMerges[u]);
  }

  for (int u = 0; u < numConcurrentUnits; ++u)
	g->put(lineOrderCacheLoads[u]);
  for (int u = 0; u < numConcurrentUnits; ++u)
	g->put(lineOrderMerges[u]);
  for (int u = 0; u < numConcurrentUnits; ++u)
	g->put(lineOrderScans[u]);
  g->put(collate);

  return g;
}

std::shared_ptr<OperatorGraph>
LocalFileSystemQueries::lineOrderScan(const std::string &dataDir, FileType fileType, int numConcurrentUnits, const std::shared_ptr<Normal> &n) {

  auto g = n->createQuery();

  auto lineorderCacheLoads = common::Operators::makeFileCacheLoadOperators("lineorder", fileType == FileType::CSV ? "lineorder.tbl" : "parquet/lineorder.snappy.parquet", SSBSchema::LineOrderFields, dataDir, numConcurrentUnits, g);
  auto lineorderScans = common::Operators::makeFileScanOperators("lineorder", fileType == FileType::CSV ? "lineorder.tbl" : "parquet/lineorder.snappy.parquet", fileType, SSBSchema::LineOrderFields, dataDir, numConcurrentUnits, g);
  auto lineorderMerges = common::Operators::makeMergeOperators("lineorder", numConcurrentUnits, g);
  auto collate = common::Operators::makeCollateOperator(g);

  common::Operators::connectHitsToEach(lineorderCacheLoads, lineorderMerges);
  common::Operators::connectMissesToEach(lineorderCacheLoads, lineorderScans);
  common::Operators::connectToEach(lineorderScans, lineorderMerges);

  common::Operators::connectToOne(lineorderMerges, collate);

  return g;
}

std::shared_ptr<OperatorGraph>
LocalFileSystemQueries::lineOrderFilter(const std::string &dataDir,
										short discount,
										short quantity,
										int numConcurrentUnits,
										const std::shared_ptr<OperatorManager> &mgr) {

  auto g = OperatorGraph::make(mgr);

  auto lineOrderCacheLoads = Operators::makeLineOrderFileCacheLoadOperators(dataDir, numConcurrentUnits, g);
  auto lineOrderScans = Operators::makeLineOrderFileScanOperators(dataDir, numConcurrentUnits, g);
  auto lineOrderMerges = Operators::makeLineOrderMergeOperators(numConcurrentUnits, g);
  auto lineOrderFilters = Operators::makeLineOrderFilterOperators(discount, quantity, true, numConcurrentUnits, g);
  auto collate = Operators::makeCollateOperator(g);

  // Wire up
  for (int u = 0; u < numConcurrentUnits; ++u) {
	lineOrderCacheLoads[u]->setHitOperator(lineOrderMerges[u]);
	lineOrderMerges[u]->consume(lineOrderCacheLoads[u]);
  }

  for (int u = 0; u < numConcurrentUnits; ++u) {
	lineOrderCacheLoads[u]->setMissOperator(lineOrderScans[u]);
	lineOrderScans[u]->consume(lineOrderCacheLoads[u]);
  }

  for (int u = 0; u < numConcurrentUnits; ++u) {
	lineOrderScans[u]->produce(lineOrderMerges[u]);
	lineOrderMerges[u]->consume(lineOrderScans[u]);
  }

  for (int u = 0; u < numConcurrentUnits; ++u) {
	lineOrderMerges[u]->produce(lineOrderFilters[u]);
	lineOrderFilters[u]->consume(lineOrderMerges[u]);
  }

  for (int u = 0; u < numConcurrentUnits; ++u) {
	lineOrderFilters[u]->produce(collate);
	collate->consume(lineOrderFilters[u]);
  }

  for (int u = 0; u < numConcurrentUnits; ++u)
	g->put(lineOrderCacheLoads[u]);
  for (int u = 0; u < numConcurrentUnits; ++u)
	g->put(lineOrderMerges[u]);
  for (int u = 0; u < numConcurrentUnits; ++u)
	g->put(lineOrderScans[u]);
  for (int u = 0; u < numConcurrentUnits; ++u)
	g->put(lineOrderFilters[u]);
  g->put(collate);

  return g;
}

std::shared_ptr<OperatorGraph>
LocalFileSystemQueries::join(const std::string &dataDir,
							 short year,
							 short discount,
							 short quantity,
							 int numConcurrentUnits,
							 const std::shared_ptr<OperatorManager> &mgr) {

  auto g = OperatorGraph::make(mgr);

  auto dateCacheLoads = Operators::makeDateFileCacheLoadOperators(dataDir, numConcurrentUnits, g);
  auto dateScans = Operators::makeDateFileScanOperators(dataDir, numConcurrentUnits, g);
  auto dateMerges = Operators::makeDateMergeOperators(numConcurrentUnits, g);
  auto lineOrderCacheLoads = Operators::makeLineOrderFileCacheLoadOperators(dataDir, numConcurrentUnits, g);
  auto lineOrderScans = Operators::makeLineOrderFileScanOperators(dataDir, numConcurrentUnits, g);
  auto lineOrderMerges = Operators::makeLineOrderMergeOperators(numConcurrentUnits, g);
  auto dateFilters = Operators::makeDateFilterOperators(year, true, numConcurrentUnits, g);
  auto lineOrderFilters = Operators::makeLineOrderFilterOperators(discount, quantity, true, numConcurrentUnits, g);
  auto dateShuffles = Operators::makeDateShuffleOperators(numConcurrentUnits, g);
  auto lineOrderShuffles = Operators::makeLineOrderShuffleOperators(numConcurrentUnits, g);
  auto joinBuild = Operators::makeHashJoinBuildOperators(numConcurrentUnits, g);
  auto joinProbe = Operators::makeHashJoinProbeOperators(numConcurrentUnits, g);
  auto collate = Operators::makeCollateOperator(g);

  // Wire up
  for (int u = 0; u < numConcurrentUnits; ++u) {
	dateCacheLoads[u]->setHitOperator(dateMerges[u]);
	dateMerges[u]->consume(dateCacheLoads[u]);
  }

  for (int u = 0; u < numConcurrentUnits; ++u) {
	dateCacheLoads[u]->setMissOperator(dateScans[u]);
	dateScans[u]->consume(dateCacheLoads[u]);
  }

  for (int u = 0; u < numConcurrentUnits; ++u) {
	dateScans[u]->produce(dateMerges[u]);
	dateMerges[u]->consume(dateScans[u]);
  }

  for (int u = 0; u < numConcurrentUnits; ++u) {
	dateMerges[u]->produce(dateFilters[u]);
	dateFilters[u]->consume(dateMerges[u]);
  }

  for (int u = 0; u < numConcurrentUnits; ++u) {
	lineOrderCacheLoads[u]->setHitOperator(lineOrderMerges[u]);
	lineOrderMerges[u]->consume(lineOrderCacheLoads[u]);
  }

  for (int u = 0; u < numConcurrentUnits; ++u) {
	lineOrderCacheLoads[u]->setMissOperator(lineOrderScans[u]);
	lineOrderScans[u]->consume(lineOrderCacheLoads[u]);
  }

  for (int u = 0; u < numConcurrentUnits; ++u) {
	lineOrderScans[u]->produce(lineOrderMerges[u]);
	lineOrderMerges[u]->consume(lineOrderScans[u]);
  }

  for (int u = 0; u < numConcurrentUnits; ++u) {
	lineOrderMerges[u]->produce(lineOrderFilters[u]);
	lineOrderFilters[u]->consume(lineOrderMerges[u]);
  }

  for (int u = 0; u < numConcurrentUnits; ++u) {
	dateFilters[u]->produce(dateShuffles[u]);
	dateShuffles[u]->consume(dateFilters[u]);
  }

  for (int u = 0; u < numConcurrentUnits; ++u) {
	lineOrderFilters[u]->produce(lineOrderShuffles[u]);
	lineOrderShuffles[u]->consume(lineOrderFilters[u]);
  }

  for (int u1 = 0; u1 < numConcurrentUnits; ++u1) {
	for (int u2 = 0; u2 < numConcurrentUnits; ++u2) {
	  dateShuffles[u1]->produce(joinBuild[u2]);
	  joinBuild[u2]->consume(dateShuffles[u1]);
	}
  }

  for (int u1 = 0; u1 < numConcurrentUnits; ++u1) {
	for (int u2 = 0; u2 < numConcurrentUnits; ++u2) {
	  lineOrderShuffles[u1]->produce(joinProbe[u2]);
	  joinProbe[u2]->consume(lineOrderShuffles[u1]);
	}
  }

  for (int u = 0; u < numConcurrentUnits; ++u) {
	joinBuild[u]->produce(joinProbe[u]);
	joinProbe[u]->consume(joinBuild[u]);
  }

  for (int u = 0; u < numConcurrentUnits; ++u) {
	joinProbe[u]->produce(collate);
	collate->consume(joinProbe[u]);
  }

  for (int u = 0; u < numConcurrentUnits; ++u)
	g->put(dateCacheLoads[u]);
  for (int u = 0; u < numConcurrentUnits; ++u)
	g->put(dateMerges[u]);
  for (int u = 0; u < numConcurrentUnits; ++u)
	g->put(dateScans[u]);
  for (int u = 0; u < numConcurrentUnits; ++u)
	g->put(lineOrderCacheLoads[u]);
  for (int u = 0; u < numConcurrentUnits; ++u)
	g->put(lineOrderMerges[u]);
  for (int u = 0; u < numConcurrentUnits; ++u)
	g->put(lineOrderScans[u]);
  for (int u = 0; u < numConcurrentUnits; ++u)
	g->put(dateFilters[u]);
  for (int u = 0; u < numConcurrentUnits; ++u)
	g->put(lineOrderFilters[u]);
  for (int u = 0; u < numConcurrentUnits; ++u)
	g->put(dateShuffles[u]);
  for (int u = 0; u < numConcurrentUnits; ++u)
	g->put(lineOrderShuffles[u]);
  for (int u = 0; u < numConcurrentUnits; ++u)
	g->put(joinBuild[u]);
  for (int u = 0; u < numConcurrentUnits; ++u)
	g->put(joinProbe[u]);
  g->put(collate);

  return g;
}

std::shared_ptr<OperatorGraph>
LocalFileSystemQueries::full(const std::string &dataDir,
							 FileType fileType,
							 short year,
							 short discount,
							 short quantity,
							 int numConcurrentUnits,
							 const std::shared_ptr<OperatorManager> &mgr) {

  auto g = OperatorGraph::make(mgr);

  auto dateCacheLoads = common::Operators::makeFileCacheLoadOperators("date", fileType == FileType::CSV ? "date.tbl" : "parquet/date.snappy.parquet", SSBSchema::DateFields, dataDir, numConcurrentUnits, g);
  auto dateScans = common::Operators::makeFileScanOperators("date", fileType == FileType::CSV ? "date.tbl" : "parquet/date.snappy.parquet", fileType, SSBSchema::DateFields, dataDir, numConcurrentUnits, g);
  auto dateMerges = common::Operators::makeMergeOperators("date", numConcurrentUnits, g);
  auto lineOrderCacheLoads = common::Operators::makeFileCacheLoadOperators("lineorder", fileType == FileType::CSV ? "lineorder.tbl" : "parquet/lineorder.snappy.parquet", SSBSchema::LineOrderFields, dataDir, numConcurrentUnits, g);
  auto lineOrderScans = common::Operators::makeFileScanOperators("lineorder", fileType == FileType::CSV ? "lineorder.tbl" : "parquet/lineorder.snappy.parquet", fileType, SSBSchema::LineOrderFields, dataDir, numConcurrentUnits, g);
  auto lineOrderMerges = common::Operators::makeMergeOperators("lineorder", numConcurrentUnits, g);
  auto dateFilters = Operators::makeDateFilterOperators(year, fileType == FileType::CSV, numConcurrentUnits, g);
  auto lineOrderFilters = Operators::makeLineOrderFilterOperators(discount, quantity, fileType == FileType::CSV, numConcurrentUnits, g);
  auto dateShuffles = common::Operators::makeShuffleOperators("date", "d_datekey", numConcurrentUnits, g);
  auto lineOrderShuffles = common::Operators::makeShuffleOperators("lineorder", "lo_orderdate", numConcurrentUnits, g);
  auto joinBuild =  common::Operators::makeHashJoinBuildOperators("1", "d_datekey", numConcurrentUnits, g);
  auto joinProbe = common::Operators::makeHashJoinProbeOperators("1", "d_datekey", "lo_orderdate", numConcurrentUnits, g);
  auto aggregates = Operators::makeAggregateOperators(numConcurrentUnits, g);
  auto aggregateReduce = Operators::makeAggregateReduceOperator(g);
  auto collate = common::Operators::makeCollateOperator(g);

  common::Operators::connectHitsToEach(dateCacheLoads, dateMerges);
  common::Operators::connectMissesToEach(dateCacheLoads, dateScans);
  common::Operators::connectToEach(dateScans, dateMerges);

  common::Operators::connectHitsToEach(lineOrderCacheLoads, lineOrderMerges);
  common::Operators::connectMissesToEach(lineOrderCacheLoads, lineOrderScans);
  common::Operators::connectToEach(lineOrderScans, lineOrderMerges);

  common::Operators::connectToEach(dateMerges, dateFilters);
  common::Operators::connectToEach(lineOrderMerges, lineOrderFilters);

  common::Operators::connectToEach(dateFilters, dateShuffles);
  common::Operators::connectToEach(lineOrderFilters, lineOrderShuffles);

  common::Operators::connectToAll(dateShuffles, joinBuild);
  common::Operators::connectToAll(lineOrderShuffles, joinProbe);
  common::Operators::connectToEach(joinBuild, joinProbe);

  common::Operators::connectToEach(joinProbe, aggregates);
  common::Operators::connectToOne(aggregates, aggregateReduce);

  common::Operators::connectToOne(aggregateReduce, collate);

//  // Wire up
//  for (int u = 0; u < numConcurrentUnits; ++u) {
//	dateCacheLoads[u]->setHitOperator(dateMerges[u]);
//	dateMerges[u]->consume(dateCacheLoads[u]);
//  }
//
//  for (int u = 0; u < numConcurrentUnits; ++u) {
//	dateCacheLoads[u]->setMissOperator(dateScans[u]);
//	dateScans[u]->consume(dateCacheLoads[u]);
//  }
//
//  for (int u = 0; u < numConcurrentUnits; ++u) {
//	dateScans[u]->produce(dateMerges[u]);
//	dateMerges[u]->consume(dateScans[u]);
//  }
//
//  for (int u = 0; u < numConcurrentUnits; ++u) {
//	dateMerges[u]->produce(dateFilters[u]);
//	dateFilters[u]->consume(dateMerges[u]);
//  }
//
//  for (int u = 0; u < numConcurrentUnits; ++u) {
//	lineOrderCacheLoads[u]->setHitOperator(lineOrderMerges[u]);
//	lineOrderMerges[u]->consume(lineOrderCacheLoads[u]);
//  }
//
//  for (int u = 0; u < numConcurrentUnits; ++u) {
//	lineOrderCacheLoads[u]->setMissOperator(lineOrderScans[u]);
//	lineOrderScans[u]->consume(lineOrderCacheLoads[u]);
//  }
//
//  for (int u = 0; u < numConcurrentUnits; ++u) {
//	lineOrderScans[u]->produce(lineOrderMerges[u]);
//	lineOrderMerges[u]->consume(lineOrderScans[u]);
//  }
//
//  for (int u = 0; u < numConcurrentUnits; ++u) {
//	lineOrderMerges[u]->produce(lineOrderFilters[u]);
//	lineOrderFilters[u]->consume(lineOrderMerges[u]);
//  }
//
//  for (int u = 0; u < numConcurrentUnits; ++u) {
//	dateFilters[u]->produce(dateShuffles[u]);
//	dateShuffles[u]->consume(dateFilters[u]);
//  }
//
//  for (int u = 0; u < numConcurrentUnits; ++u) {
//	lineOrderFilters[u]->produce(lineOrderShuffles[u]);
//	lineOrderShuffles[u]->consume(lineOrderFilters[u]);
//  }
//
//  for (int u1 = 0; u1 < numConcurrentUnits; ++u1) {
//	for (int u2 = 0; u2 < numConcurrentUnits; ++u2) {
//	  dateShuffles[u1]->produce(joinBuild[u2]);
//	  joinBuild[u2]->consume(dateShuffles[u1]);
//	}
//  }
//
//  for (int u1 = 0; u1 < numConcurrentUnits; ++u1) {
//	for (int u2 = 0; u2 < numConcurrentUnits; ++u2) {
//	  lineOrderShuffles[u1]->produce(joinProbe[u2]);
//	  joinProbe[u2]->consume(lineOrderShuffles[u1]);
//	}
//  }
//
//  for (int u = 0; u < numConcurrentUnits; ++u) {
//	joinBuild[u]->produce(joinProbe[u]);
//	joinProbe[u]->consume(joinBuild[u]);
//  }
//
//  for (int u = 0; u < numConcurrentUnits; ++u) {
//	joinProbe[u]->produce(aggregates[u]);
//	aggregates[u]->consume(joinProbe[u]);
//  }
//
//  for (int u = 0; u < numConcurrentUnits; ++u) {
//	aggregates[u]->produce(aggregateReduce);
//	aggregateReduce->consume(aggregates[u]);
//  }
//
//  aggregateReduce->produce(collate);
//  collate->consume(aggregateReduce);
//
//  for (int u = 0; u < numConcurrentUnits; ++u)
//	g->put(dateCacheLoads[u]);
//  for (int u = 0; u < numConcurrentUnits; ++u)
//	g->put(dateMerges[u]);
//  for (int u = 0; u < numConcurrentUnits; ++u)
//	g->put(dateScans[u]);
//  for (int u = 0; u < numConcurrentUnits; ++u)
//	g->put(lineOrderCacheLoads[u]);
//  for (int u = 0; u < numConcurrentUnits; ++u)
//	g->put(lineOrderMerges[u]);
//  for (int u = 0; u < numConcurrentUnits; ++u)
//	g->put(lineOrderScans[u]);
  for (int u = 0; u < numConcurrentUnits; ++u)
	g->put(dateFilters[u]);
  for (int u = 0; u < numConcurrentUnits; ++u)
	g->put(lineOrderFilters[u]);
//  for (int u = 0; u < numConcurrentUnits; ++u)
//	g->put(dateShuffles[u]);
//  for (int u = 0; u < numConcurrentUnits; ++u)
//	g->put(lineOrderShuffles[u]);
//  for (int u = 0; u < numConcurrentUnits; ++u)
//	g->put(joinBuild[u]);
//  for (int u = 0; u < numConcurrentUnits; ++u)
//	g->put(joinProbe[u]);
  for (int u = 0; u < numConcurrentUnits; ++u)
	g->put(aggregates[u]);
  g->put(aggregateReduce);
//  g->put(collate);

  return g;
}

std::shared_ptr<OperatorGraph>
LocalFileSystemQueries::bloom(const std::string &dataDir,
							 short year,
							 short discount,
							 short quantity,
							 int numConcurrentUnits,
							 const std::shared_ptr<Normal> &n) {

  auto g = n->createQuery();

  auto dateCacheLoads = Operators::makeDateFileCacheLoadOperators(dataDir, numConcurrentUnits, g);
  auto dateScans = Operators::makeDateFileScanOperators(dataDir, numConcurrentUnits, g);
  auto dateMerges = Operators::makeDateMergeOperators(numConcurrentUnits, g);
  auto dateBloomCreate = Operators::makeDateBloomCreateOperators(g);
  auto lineOrderCacheLoads = Operators::makeLineOrderFileCacheLoadOperators(dataDir, numConcurrentUnits, g);
  auto lineOrderScanBloomUses = Operators::makeLineOrderFileScanBloomUseOperators(dataDir, numConcurrentUnits, g);
  auto lineOrderMerges = Operators::makeLineOrderMergeOperators(numConcurrentUnits, g);
  auto dateFilters = Operators::makeDateFilterOperators(year, true, numConcurrentUnits, g);
  auto lineOrderFilters = Operators::makeLineOrderFilterOperators(discount, true, quantity, numConcurrentUnits, g);
  auto dateShuffles = Operators::makeDateShuffleOperators(numConcurrentUnits, g);
  auto lineOrderShuffles = Operators::makeLineOrderShuffleOperators(numConcurrentUnits, g);
  auto joinBuild = Operators::makeHashJoinBuildOperators(numConcurrentUnits, g);
  auto joinProbe = Operators::makeHashJoinProbeOperators(numConcurrentUnits, g);
  auto aggregates = Operators::makeAggregateOperators(numConcurrentUnits, g);
  auto aggregateReduce = Operators::makeAggregateReduceOperator(g);
  auto collate = Operators::makeCollateOperator(g);

  // Wire up
  for (int u = 0; u < numConcurrentUnits; ++u) {
	dateCacheLoads[u]->setHitOperator(dateMerges[u]);
	dateMerges[u]->consume(dateCacheLoads[u]);
  }

  for (int u = 0; u < numConcurrentUnits; ++u) {
	dateCacheLoads[u]->setMissOperator(dateScans[u]);
	dateScans[u]->consume(dateCacheLoads[u]);
  }

  for (int u = 0; u < numConcurrentUnits; ++u) {
	dateScans[u]->produce(dateMerges[u]);
	dateMerges[u]->consume(dateScans[u]);
  }

  for (int u = 0; u < numConcurrentUnits; ++u) {
	dateMerges[u]->produce(dateFilters[u]);
	dateFilters[u]->consume(dateMerges[u]);
  }

  for (int u = 0; u < numConcurrentUnits; ++u) {
	dateFilters[u]->produce(dateBloomCreate);
	dateBloomCreate->consume(dateFilters[u]);
  }

  for (int u = 0; u < numConcurrentUnits; ++u) {
	lineOrderCacheLoads[u]->setHitOperator(lineOrderMerges[u]);
	lineOrderMerges[u]->consume(lineOrderCacheLoads[u]);
  }

  for (int u = 0; u < numConcurrentUnits; ++u) {
	lineOrderCacheLoads[u]->setMissOperator(lineOrderScanBloomUses[u]);
	lineOrderScanBloomUses[u]->consume(lineOrderCacheLoads[u]);
  }

  for (int u = 0; u < numConcurrentUnits; ++u) {
	dateBloomCreate->produce(lineOrderScanBloomUses[u]);
	lineOrderScanBloomUses[u]->consume(dateBloomCreate);
  }

  for (int u = 0; u < numConcurrentUnits; ++u) {
	lineOrderScanBloomUses[u]->produce(lineOrderMerges[u]);
	lineOrderMerges[u]->consume(lineOrderScanBloomUses[u]);
  }

  for (int u = 0; u < numConcurrentUnits; ++u) {
	lineOrderMerges[u]->produce(lineOrderFilters[u]);
	lineOrderFilters[u]->consume(lineOrderMerges[u]);
  }

  for (int u = 0; u < numConcurrentUnits; ++u) {
	dateFilters[u]->produce(dateShuffles[u]);
	dateShuffles[u]->consume(dateFilters[u]);
  }

  for (int u = 0; u < numConcurrentUnits; ++u) {
	lineOrderFilters[u]->produce(lineOrderShuffles[u]);
	lineOrderShuffles[u]->consume(lineOrderFilters[u]);
  }

  for (int u1 = 0; u1 < numConcurrentUnits; ++u1) {
	for (int u2 = 0; u2 < numConcurrentUnits; ++u2) {
	  dateShuffles[u1]->produce(joinBuild[u2]);
	  joinBuild[u2]->consume(dateShuffles[u1]);
	}
  }

  for (int u1 = 0; u1 < numConcurrentUnits; ++u1) {
	for (int u2 = 0; u2 < numConcurrentUnits; ++u2) {
	  lineOrderShuffles[u1]->produce(joinProbe[u2]);
	  joinProbe[u2]->consume(lineOrderShuffles[u1]);
	}
  }

  for (int u = 0; u < numConcurrentUnits; ++u) {
	joinBuild[u]->produce(joinProbe[u]);
	joinProbe[u]->consume(joinBuild[u]);
  }

  for (int u = 0; u < numConcurrentUnits; ++u) {
	joinProbe[u]->produce(aggregates[u]);
	aggregates[u]->consume(joinProbe[u]);
  }

  for (int u = 0; u < numConcurrentUnits; ++u) {
	aggregates[u]->produce(aggregateReduce);
	aggregateReduce->consume(aggregates[u]);
  }

  aggregateReduce->produce(collate);
  collate->consume(aggregateReduce);

  for (int u = 0; u < numConcurrentUnits; ++u)
	g->put(dateCacheLoads[u]);
  for (int u = 0; u < numConcurrentUnits; ++u)
	g->put(dateMerges[u]);
  for (int u = 0; u < numConcurrentUnits; ++u)
	g->put(dateScans[u]);
  g->put(dateBloomCreate);
  for (int u = 0; u < numConcurrentUnits; ++u)
	g->put(lineOrderCacheLoads[u]);
  for (int u = 0; u < numConcurrentUnits; ++u)
	g->put(lineOrderMerges[u]);
  for (int u = 0; u < numConcurrentUnits; ++u)
	g->put(lineOrderScanBloomUses[u]);
  for (int u = 0; u < numConcurrentUnits; ++u)
	g->put(dateFilters[u]);
  for (int u = 0; u < numConcurrentUnits; ++u)
	g->put(lineOrderFilters[u]);
  for (int u = 0; u < numConcurrentUnits; ++u)
	g->put(dateShuffles[u]);
  for (int u = 0; u < numConcurrentUnits; ++u)
	g->put(lineOrderShuffles[u]);
  for (int u = 0; u < numConcurrentUnits; ++u)
	g->put(joinBuild[u]);
  for (int u = 0; u < numConcurrentUnits; ++u)
	g->put(joinProbe[u]);
  for (int u = 0; u < numConcurrentUnits; ++u)
	g->put(aggregates[u]);
  g->put(aggregateReduce);
  g->put(collate);

  return g;
}
