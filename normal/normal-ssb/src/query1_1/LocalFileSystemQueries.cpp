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
  auto joinBuild = Operators::makeHashJoinBuildOperators();
  auto joinProbe = Operators::makeHashJoinProbeOperators();
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
	dateFilters[u]->produce(joinBuild);
	joinBuild->consume(dateFilters[u]);
  }

  joinBuild->produce(joinProbe);
  joinProbe->consume(joinBuild);

  for (int u = 0; u < numConcurrentUnits; ++u) {
	lineOrderFilters[u]->produce(joinProbe);
	joinProbe->consume(lineOrderFilters[u]);
  }

  joinProbe->produce(collate);
  collate->consume(joinProbe);

  for (int u = 0; u < numConcurrentUnits; ++u)
	mgr->put(dateScans[u]);
  for (int u = 0; u < numConcurrentUnits; ++u)
	mgr->put(lineOrderScans[u]);
  for (int u = 0; u < numConcurrentUnits; ++u)
	mgr->put(dateFilters[u]);
  for (int u = 0; u < numConcurrentUnits; ++u)
	mgr->put(lineOrderFilters[u]);
  mgr->put(joinBuild);
  mgr->put(joinProbe);
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
  auto joinBuild = Operators::makeHashJoinBuildOperators();
  auto joinProbe = Operators::makeHashJoinProbeOperators();
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
	dateFilters[u]->produce(joinBuild);
	joinBuild->consume(dateFilters[u]);
  }

  joinBuild->produce(joinProbe);
  joinProbe->consume(joinBuild);

  for (int u = 0; u < numConcurrentUnits; ++u) {
	lineOrderFilters[u]->produce(joinProbe);
	joinProbe->consume(lineOrderFilters[u]);
  }

  joinProbe->produce(aggregate);
  aggregate->consume(joinProbe);

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
  mgr->put(joinBuild);
  mgr->put(joinProbe);
  mgr->put(aggregate);
  mgr->put(collate);

  return mgr;
}
