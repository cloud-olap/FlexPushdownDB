//
// Created by matt on 10/8/20.
//
//
//#include "normal/ssb/query2_1/LocalFileSystemQueries.h"
//#include <normal/ssb/Globals.h>
//#include <normal/ssb/common/Operators.h>
//#include <normal/ssb/query2_1/Operators.h>
//#include <normal/core/ATTIC/Normal.h>
//#include <normal/ssb/SSBSchema.h>
//
//using namespace normal::ssb::query2_1;
//using namespace normal::ssb::common;
//using namespace normal::ssb;
//using namespace normal::pushdown::aggregate;
//using namespace normal::core::type;
//using namespace normal::expression::gandiva;
//
//
//std::shared_ptr<OperatorGraph> LocalFileSystemQueries::partFilter(const std::string &dataDir,
//																  FileType fileType,
//																  const std::string &category,
//																  int numConcurrentUnits,
//																  const std::shared_ptr<Normal> &n) {
//
//  auto g = n->createQuery();
//
//  auto partCacheLoads = common::Operators::makeFileCacheLoadOperators("part", fileType == FileType::CSV ? "part.tbl" : "parquet/part.snappy.parquet", SSBSchema::part()->field_names(), dataDir, numConcurrentUnits, g);
//  auto partScans = common::Operators::makeFileScanOperators("part", "part.tbl", fileType, SSBSchema::part()->field_names(), dataDir, numConcurrentUnits, g);
//  auto partMerges = common::Operators::makeMergeOperators("part", numConcurrentUnits, g);
//
//  auto partFilters = Operators::makePartFilterOperators(category, numConcurrentUnits, g);
//
//  auto collate = common::Operators::makeCollateOperator(g);
//
//  common::Operators::connectHitsToEach(partCacheLoads, partMerges);
//  common::Operators::connectMissesToEach(partCacheLoads, partScans);
//  common::Operators::connectToEach(partScans, partMerges);
//
//  common::Operators::connectToEach(partMerges, partFilters);
//
//  common::Operators::connectToOne(partFilters, collate);
//
//  return g;
//}
//
//
//std::shared_ptr<OperatorGraph> LocalFileSystemQueries::join2x(const std::string &dataDir,
//															  FileType fileType,
//															  const std::string& region,
//															  int numConcurrentUnits,
//															  const std::shared_ptr<Normal> &n) {
//
//  auto g = n->createQuery();
//
//  auto supplierCacheLoads = common::Operators::makeFileCacheLoadOperators("supplier", "supplier.tbl", SupplierFields, dataDir, numConcurrentUnits, g);
//  auto supplierScans = common::Operators::makeFileScanOperators("supplier", "supplier.tbl", fileType, SupplierFields, dataDir, numConcurrentUnits, g);
//  auto supplierMerges = common::Operators::makeMergeOperators("supplier", numConcurrentUnits, g);
//
//  auto lineOrderCacheLoads = common::Operators::makeFileCacheLoadOperators("lineorder", "lineorder.tbl", LineOrderFields, dataDir, numConcurrentUnits, g);
//  auto lineOrderScans = common::Operators::makeFileScanOperators("lineorder", "lineorder.tbl", fileType, LineOrderFields, dataDir, numConcurrentUnits, g);
//  auto lineOrderMerges = common::Operators::makeMergeOperators("lineorder", numConcurrentUnits, g);
//
//  auto supplierFilters = Operators::makeSupplierFilterOperators(region, numConcurrentUnits, g);
//
//  auto supplierShuffles = common::Operators::makeShuffleOperators("s_suppkey", "s_suppkey", numConcurrentUnits, g);
//  auto lineOrderSupplierKeyShuffles = common::Operators::makeShuffleOperators("lo_suppkey", "lo_suppkey", numConcurrentUnits, g);
//  auto join1Build = common::Operators::makeHashJoinBuildOperators("s->lo", "s_suppkey", numConcurrentUnits, g);
//  auto join1Probe = common::Operators::makeHashJoinProbeOperators("s->lo", "s_suppkey", "lo_suppkey", numConcurrentUnits, g);
//
//  auto collate = common::Operators::makeCollateOperator(g);
//
//  // Wire up
//  common::Operators::connectHitsToEach(supplierCacheLoads, supplierMerges);
//  common::Operators::connectMissesToEach(supplierCacheLoads, supplierScans);
//  common::Operators::connectToEach(supplierScans, supplierMerges);
//
//  common::Operators::connectHitsToEach(lineOrderCacheLoads, lineOrderMerges);
//  common::Operators::connectMissesToEach(lineOrderCacheLoads, lineOrderScans);
//  common::Operators::connectToEach(lineOrderScans, lineOrderMerges);
//
//  common::Operators::connectToEach(supplierMerges, supplierFilters);
//
//  common::Operators::connectToEach(supplierFilters, supplierShuffles);
//  common::Operators::connectToEach(lineOrderMerges, lineOrderSupplierKeyShuffles);
//  common::Operators::connectToAll(supplierShuffles, join1Build);
//  common::Operators::connectToAll(lineOrderSupplierKeyShuffles, join1Probe);
//  common::Operators::connectToEach(join1Build, join1Probe);
//
//  common::Operators::connectToOne(join1Probe, collate);
//
//  return g;
//}
//
//
//std::shared_ptr<OperatorGraph> LocalFileSystemQueries::join3x(const std::string &dataDir,
//															  FileType fileType,
//							 const std::string& region,
//							 int numConcurrentUnits,
//							 const std::shared_ptr<Normal> &n) {
//
//  auto g = n->createQuery();
//
//  auto supplierCacheLoads = common::Operators::makeFileCacheLoadOperators("supplier", "supplier.tbl", SupplierFields, dataDir, numConcurrentUnits, g);
//  auto supplierScans = common::Operators::makeFileScanOperators("supplier", "supplier.tbl", fileType, SupplierFields, dataDir, numConcurrentUnits, g);
//  auto supplierMerges = common::Operators::makeMergeOperators("supplier", numConcurrentUnits, g);
//
//  auto lineOrderCacheLoads = common::Operators::makeFileCacheLoadOperators("lineorder", "lineorder.tbl", LineOrderFields, dataDir, numConcurrentUnits, g);
//  auto lineOrderScans = common::Operators::makeFileScanOperators("lineorder", "lineorder.tbl", fileType,LineOrderFields, dataDir, numConcurrentUnits, g);
//  auto lineOrderMerges = common::Operators::makeMergeOperators("lineorder", numConcurrentUnits, g);
//
//  auto dateCacheLoads = common::Operators::makeFileCacheLoadOperators("date", "date.tbl", DateFields, dataDir, numConcurrentUnits, g);
//  auto dateScans = common::Operators::makeFileScanOperators("date", "date.tbl", fileType,DateFields, dataDir, numConcurrentUnits, g);
//  auto dateMerges =common:: Operators::makeMergeOperators("date", numConcurrentUnits, g);
//
//  auto supplierFilters = Operators::makeSupplierFilterOperators(region, numConcurrentUnits, g);
//
//  auto supplierShuffles = common::Operators::makeShuffleOperators("s_suppkey", "s_suppkey", numConcurrentUnits, g);
//  auto lineOrderSupplierKeyShuffles = common::Operators::makeShuffleOperators("lo_suppkey", "lo_suppkey", numConcurrentUnits, g);
//  auto join1Build = common::Operators::makeHashJoinBuildOperators("s->lo", "s_suppkey", numConcurrentUnits, g);
//  auto join1Probe = common::Operators::makeHashJoinProbeOperators("s->lo", "s_suppkey", "lo_suppkey", numConcurrentUnits, g);
//
//  auto dateShuffles = common::Operators::makeShuffleOperators("d_datekey", "d_datekey", numConcurrentUnits, g);
//  auto lineOrderDateKeyShuffles = common::Operators::makeShuffleOperators("lo_orderdate", "lo_orderdate", numConcurrentUnits, g);
//  auto join2Build = common::Operators::makeHashJoinBuildOperators("(s->lo)->d", "lo_orderdate", numConcurrentUnits, g);
//  auto join2Probe = common::Operators::makeHashJoinProbeOperators("(s->lo)->d", "lo_orderdate", "d_datekey", numConcurrentUnits, g);
//
//  auto collate = common::Operators::makeCollateOperator(g);
//
//  // Wire up
//  common::Operators::connectHitsToEach(supplierCacheLoads, supplierMerges);
//  common::Operators::connectMissesToEach(supplierCacheLoads, supplierScans);
//  common::Operators::connectToEach(supplierScans, supplierMerges);
//
//  common::Operators::connectHitsToEach(lineOrderCacheLoads, lineOrderMerges);
//  common::Operators::connectMissesToEach(lineOrderCacheLoads, lineOrderScans);
//  common::Operators::connectToEach(lineOrderScans, lineOrderMerges);
//
//  common::Operators::connectHitsToEach(dateCacheLoads, dateMerges);
//  common::Operators::connectMissesToEach(dateCacheLoads, dateScans);
//  common::Operators::connectToEach(dateScans, dateMerges);
//
//  common::Operators::connectToEach(supplierMerges, supplierFilters);
//
//  common::Operators::connectToEach(supplierFilters, supplierShuffles);
//  common::Operators::connectToEach(lineOrderMerges, lineOrderSupplierKeyShuffles);
//  common::Operators::connectToAll(supplierShuffles, join1Build);
//  common::Operators::connectToAll(lineOrderSupplierKeyShuffles, join1Probe);
//  common::Operators::connectToEach(join1Build, join1Probe);
//
//  common::Operators::connectToEach(join1Probe, lineOrderDateKeyShuffles);
//  common::Operators::connectToEach(dateMerges, dateShuffles);
//  common::Operators::connectToAll(lineOrderDateKeyShuffles, join2Build);
//  common::Operators::connectToAll(dateShuffles, join2Probe);
//  common::Operators::connectToEach(join2Build, join2Probe);
//
//  common::Operators::connectToOne(join2Probe, collate);
//
//  return g;
//}
//
//std::shared_ptr<OperatorGraph>
//LocalFileSystemQueries::join(const std::string &dataDir,
//							 FileType fileType,
//							 const std::string& category,
//							 const std::string& region,
//							 int numConcurrentUnits,
//							 const std::shared_ptr<Normal> &n) {
//
//  auto g = n->createQuery();
//
//  auto supplierCacheLoads = common::Operators::makeFileCacheLoadOperators("supplier", "supplier.tbl", SSBSchema::supplier()->field_names(), dataDir, numConcurrentUnits, g);
//  auto supplierScans = common::Operators::makeFileScanOperators("supplier", "supplier.tbl", fileType, SSBSchema::supplier()->field_names(), dataDir, numConcurrentUnits, g);
//  auto supplierMerges = common::Operators::makeMergeOperators("supplier", numConcurrentUnits, g);
//
//  auto lineOrderCacheLoads = common::Operators::makeFileCacheLoadOperators("lineorder", "lineorder.tbl", SSBSchema::lineOrder()->field_names(), dataDir, numConcurrentUnits, g);
//  auto lineOrderScans = common::Operators::makeFileScanOperators("lineorder", "lineorder.tbl", fileType,SSBSchema::lineOrder()->field_names(), dataDir, numConcurrentUnits, g);
//  auto lineOrderMerges = common::Operators::makeMergeOperators("lineorder", numConcurrentUnits, g);
//
//  auto dateCacheLoads = common::Operators::makeFileCacheLoadOperators("date", "date.tbl",
//																	  SSBSchema::date()->field_names(), dataDir, numConcurrentUnits, g);
//  auto dateScans = common::Operators::makeFileScanOperators("date", "date.tbl", fileType,
//															SSBSchema::date()->field_names(), dataDir, numConcurrentUnits, g);
//  auto dateMerges =common:: Operators::makeMergeOperators("date", numConcurrentUnits, g);
//
//  auto partCacheLoads = common::Operators::makeFileCacheLoadOperators("part", "part.tbl", SSBSchema::part()->field_names(), dataDir, numConcurrentUnits, g);
//  auto partScans = common::Operators::makeFileScanOperators("part", "part.tbl", fileType,SSBSchema::part()->field_names(), dataDir, numConcurrentUnits, g);
//  auto partMerges = common::Operators::makeMergeOperators("part", numConcurrentUnits, g);
//
//  auto supplierFilters = Operators::makeSupplierFilterOperators(region, numConcurrentUnits, g);
//  auto partFilters = Operators::makePartFilterOperators(category, numConcurrentUnits, g);
//
//  auto supplierShuffles = common::Operators::makeShuffleOperators("s_suppkey", "s_suppkey", numConcurrentUnits, g);
//  auto lineOrderSupplierKeyShuffles = common::Operators::makeShuffleOperators("lo_suppkey", "lo_suppkey", numConcurrentUnits, g);
//  auto join1Build = common::Operators::makeHashJoinBuildOperators("s->lo", "s_suppkey", numConcurrentUnits, g);
//  auto join1Probe = common::Operators::makeHashJoinProbeOperators("s->lo", "s_suppkey", "lo_suppkey", numConcurrentUnits, g);
//
//  auto dateShuffles = common::Operators::makeShuffleOperators("d_datekey", "d_datekey", numConcurrentUnits, g);
//  auto lineOrderDateKeyShuffles = common::Operators::makeShuffleOperators("lo_orderdate", "lo_orderdate", numConcurrentUnits, g);
//  auto join2Build = common::Operators::makeHashJoinBuildOperators("(s->lo)->d", "lo_orderdate", numConcurrentUnits, g);
//  auto join2Probe = common::Operators::makeHashJoinProbeOperators("(s->lo)->d", "lo_orderdate", "d_datekey", numConcurrentUnits, g);
//
//  auto partShuffles = common::Operators::makeShuffleOperators("p_partkey", "p_partkey", numConcurrentUnits, g);
//  auto lineOrderPartKeyShuffles = common::Operators::makeShuffleOperators("lo_partkey", "lo_partkey", numConcurrentUnits, g);
//  auto join3Build = common::Operators::makeHashJoinBuildOperators("((s->lo)->d)->p", "lo_partkey", numConcurrentUnits, g);
//  auto join3Probe = common::Operators::makeHashJoinProbeOperators("((s->lo)->d)->p", "lo_partkey", "p_partkey", numConcurrentUnits, g);
//
//  auto collate = common::Operators::makeCollateOperator(g);
//
//  // Wire up
//  common::Operators::connectHitsToEach(supplierCacheLoads, supplierMerges);
//  common::Operators::connectMissesToEach(supplierCacheLoads, supplierScans);
//  common::Operators::connectToEach(supplierScans, supplierMerges);
//
//  common::Operators::connectHitsToEach(lineOrderCacheLoads, lineOrderMerges);
//  common::Operators::connectMissesToEach(lineOrderCacheLoads, lineOrderScans);
//  common::Operators::connectToEach(lineOrderScans, lineOrderMerges);
//
//  common::Operators::connectHitsToEach(dateCacheLoads, dateMerges);
//  common::Operators::connectMissesToEach(dateCacheLoads, dateScans);
//  common::Operators::connectToEach(dateScans, dateMerges);
//
//  common::Operators::connectHitsToEach(partCacheLoads, partMerges);
//  common::Operators::connectMissesToEach(partCacheLoads, partScans);
//  common::Operators::connectToEach(partScans, partMerges);
//
//  common::Operators::connectToEach(supplierMerges, supplierFilters);
//  common::Operators::connectToEach(partMerges, partFilters);
//
//  common::Operators::connectToEach(supplierFilters, supplierShuffles);
//  common::Operators::connectToEach(lineOrderMerges, lineOrderSupplierKeyShuffles);
//  common::Operators::connectToAll(supplierShuffles, join1Build);
//  common::Operators::connectToAll(lineOrderSupplierKeyShuffles, join1Probe);
//  common::Operators::connectToEach(join1Build, join1Probe);
//
//  common::Operators::connectToEach(join1Probe, lineOrderDateKeyShuffles);
//  common::Operators::connectToEach(dateMerges, dateShuffles);
//  common::Operators::connectToAll(lineOrderDateKeyShuffles, join2Build);
//  common::Operators::connectToAll(dateShuffles, join2Probe);
//  common::Operators::connectToEach(join2Build, join2Probe);
//
//  common::Operators::connectToEach(join2Probe, lineOrderPartKeyShuffles);
//  common::Operators::connectToEach(partFilters, partShuffles);
//  common::Operators::connectToAll(lineOrderPartKeyShuffles, join3Build);
//  common::Operators::connectToAll(partShuffles, join3Probe);
//  common::Operators::connectToEach(join3Build, join3Probe);
//
//  common::Operators::connectToOne(join3Probe, collate);
//
//  return g;
//}
//
