//
// Created by matt on 26/6/20.
//

#include "normal/ssb/query1_1/LocalFileSystemQueries.h"

#include <normal/ssb/Globals.h>
#include <normal/ssb/query1_1/Operators.h>

using namespace normal::ssb::query1_1;
using namespace normal::pushdown::aggregate;
using namespace normal::core::type;
using namespace normal::expression::gandiva;

std::shared_ptr<OperatorManager>
LocalFileSystemQueries::dateScanQuery(const std::string &dataDir, int numConcurrentUnits) {

  auto mgr = std::make_shared<OperatorManager>();

  auto dateScans = Operators::makeDateFileScanOperators(dataDir, numConcurrentUnits);
  auto collate = Operators::makeCollate();

  // Wire up
  for (int u = 0; u < numConcurrentUnits; ++u) {
	dateScans[u]->produce(collate);
	collate->consume(dateScans[u]);
  }

  for (int u = 0; u < numConcurrentUnits; ++u)
	mgr->put(dateScans[u]);
  mgr->put(collate);

  return mgr;
}

std::shared_ptr<OperatorManager>
LocalFileSystemQueries::dateFilterQuery(const std::string &dataDir, short year, int numConcurrentUnits) {

  auto mgr = std::make_shared<OperatorManager>();

  auto dateScans = Operators::makeDateFileScanOperators(dataDir, numConcurrentUnits);
  auto dateFilters = Operators::makeDateFilterOperators(year, numConcurrentUnits);
  auto collate = Operators::makeCollate();

  // Wire up
  for (int u = 0; u < numConcurrentUnits; ++u) {
	dateScans[u]->produce(dateFilters[u]);
	dateFilters[u]->consume(dateScans[u]);
  }

  for (int u = 0; u < numConcurrentUnits; ++u) {
	dateFilters[u]->produce(collate);
	collate->consume(dateFilters[u]);
  }

  for (int u = 0; u < numConcurrentUnits; ++u)
	mgr->put(dateScans[u]);
  for (int u = 0; u < numConcurrentUnits; ++u)
	mgr->put(dateFilters[u]);
  mgr->put(collate);

  return mgr;
}

std::shared_ptr<OperatorManager>
LocalFileSystemQueries::lineOrderScanQuery(const std::string &dataDir,
										   int numConcurrentUnits) {

  auto mgr = std::make_shared<OperatorManager>();

  auto lineOrderScans = Operators::makeLineOrderFileScanOperators(dataDir, numConcurrentUnits);
  auto collate = Operators::makeCollate();

  // Wire up
  for (int u = 0; u < numConcurrentUnits; ++u) {
	lineOrderScans[u]->produce(collate);
	collate->consume(lineOrderScans[u]);
  }

  for (int u = 0; u < numConcurrentUnits; ++u)
	mgr->put(lineOrderScans[u]);
  mgr->put(collate);

  return mgr;
}

std::shared_ptr<OperatorManager>
LocalFileSystemQueries::lineOrderFilterQuery(const std::string &dataDir,
											 short discount,
											 short quantity,
											 int numConcurrentUnits) {

  auto mgr = std::make_shared<OperatorManager>();

  auto lineOrderScans = Operators::makeLineOrderFileScanOperators(dataDir, numConcurrentUnits);
  auto lineOrderFilters = Operators::makeLineOrderFilterOperators(discount, quantity, numConcurrentUnits);
  auto collate = Operators::makeCollate();

  // Wire up
  for (int u = 0; u < numConcurrentUnits; ++u) {
	lineOrderScans[u]->produce(lineOrderFilters[u]);
	lineOrderFilters[u]->consume(lineOrderScans[u]);
  }

  for (int u = 0; u < numConcurrentUnits; ++u) {
	lineOrderFilters[u]->produce(collate);
	collate->consume(lineOrderFilters[u]);
  }

  for (int u = 0; u < numConcurrentUnits; ++u)
	mgr->put(lineOrderScans[u]);
  for (int u = 0; u < numConcurrentUnits; ++u)
	mgr->put(lineOrderFilters[u]);
  mgr->put(collate);

  return mgr;
}

std::shared_ptr<OperatorManager>
LocalFileSystemQueries::joinQuery(const std::string &dataDir,
								  short year,
								  short discount,
								  short quantity,
								  int numConcurrentUnits) {

  auto mgr = std::make_shared<OperatorManager>();

  auto dateScans = Operators::makeDateFileScanOperators(dataDir, numConcurrentUnits);
  auto lineOrderScans = Operators::makeLineOrderFileScanOperators(dataDir, numConcurrentUnits);
  auto dateFilters = Operators::makeDateFilterOperators(year, numConcurrentUnits);
  auto lineOrderFilters = Operators::makeLineOrderFilterOperators(discount, quantity, numConcurrentUnits);
  auto dateShuffles = Operators::makeDateShuffleOperators(numConcurrentUnits);
  auto lineOrderShuffles = Operators::makeLineOrderShuffleOperators(numConcurrentUnits);
  auto joinBuild = Operators::makeHashJoinBuildOperators(numConcurrentUnits);
  auto joinProbe = Operators::makeHashJoinProbeOperators(numConcurrentUnits);
  auto collate = Operators::makeCollate();

  // Wire up
  for (int u = 0; u < numConcurrentUnits; ++u) {
	dateScans[u]->produce(dateFilters[u]);
	dateFilters[u]->consume(dateScans[u]);
  }

  for (int u = 0; u < numConcurrentUnits; ++u) {
	lineOrderScans[u]->produce(lineOrderFilters[u]);
	lineOrderFilters[u]->consume(lineOrderScans[u]);
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
	mgr->put(dateScans[u]);
  for (int u = 0; u < numConcurrentUnits; ++u)
	mgr->put(lineOrderScans[u]);
  for (int u = 0; u < numConcurrentUnits; ++u)
	mgr->put(dateFilters[u]);
  for (int u = 0; u < numConcurrentUnits; ++u)
	mgr->put(lineOrderFilters[u]);
  for (int u = 0; u < numConcurrentUnits; ++u)
	mgr->put(dateShuffles[u]);
  for (int u = 0; u < numConcurrentUnits; ++u)
	mgr->put(lineOrderShuffles[u]);
  for (int u = 0; u < numConcurrentUnits; ++u)
	mgr->put(joinBuild[u]);
  for (int u = 0; u < numConcurrentUnits; ++u)
	mgr->put(joinProbe[u]);
  mgr->put(collate);

  return mgr;
}

std::shared_ptr<OperatorManager>
LocalFileSystemQueries::fullQuery(const std::string &dataDir,
								  short year,
								  short discount,
								  short quantity,
								  int numConcurrentUnits) {

  auto mgr = std::make_shared<OperatorManager>();

  auto dateScans = Operators::makeDateFileScanOperators(dataDir, numConcurrentUnits);
  auto lineOrderScans = Operators::makeLineOrderFileScanOperators(dataDir, numConcurrentUnits);
  auto dateFilters = Operators::makeDateFilterOperators(year, numConcurrentUnits);
  auto lineOrderFilters = Operators::makeLineOrderFilterOperators(discount, quantity, numConcurrentUnits);
  auto dateShuffles = Operators::makeDateShuffleOperators(numConcurrentUnits);
  auto lineOrderShuffles = Operators::makeLineOrderShuffleOperators(numConcurrentUnits);
  auto joinBuild = Operators::makeHashJoinBuildOperators(numConcurrentUnits);
  auto joinProbe = Operators::makeHashJoinProbeOperators(numConcurrentUnits);
  auto aggregate = Operators::makeAggregateOperators();
  auto collate = Operators::makeCollate();

  // Wire up
  for (int u = 0; u < numConcurrentUnits; ++u) {
	dateScans[u]->produce(dateFilters[u]);
	dateFilters[u]->consume(dateScans[u]);
  }

  for (int u = 0; u < numConcurrentUnits; ++u) {
	lineOrderScans[u]->produce(lineOrderFilters[u]);
	lineOrderFilters[u]->consume(lineOrderScans[u]);
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
	joinProbe[u]->produce(aggregate);
	aggregate->consume(joinProbe[u]);
  }

  aggregate->produce(collate);
  collate->consume(aggregate);

  for (int u = 0; u < numConcurrentUnits; ++u)
	mgr->put(dateScans[u]);
  for (int u = 0; u < numConcurrentUnits; ++u)
	mgr->put(lineOrderScans[u]);
  for (int u = 0; u < numConcurrentUnits; ++u)
	mgr->put(dateFilters[u]);
  for (int u = 0; u < numConcurrentUnits; ++u)
	mgr->put(lineOrderFilters[u]);
  for (int u = 0; u < numConcurrentUnits; ++u)
  	mgr->put(dateShuffles[u]);
  for (int u = 0; u < numConcurrentUnits; ++u)
	mgr->put(lineOrderShuffles[u]);
  for (int u = 0; u < numConcurrentUnits; ++u)
	mgr->put(joinBuild[u]);
  for (int u = 0; u < numConcurrentUnits; ++u)
	mgr->put(joinProbe[u]);
  mgr->put(aggregate);
  mgr->put(collate);

  return mgr;
}
