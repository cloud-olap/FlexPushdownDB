//
// Created by matt on 26/6/20.
//
//
//#include "normal/ssb/query1_1/LocalFileSystemQueries.h"
//
//#include <normal/ssb/Globals.h>
//#include <normal/ssb/query1_1/Operators.h>
//#include <normal/core/ATTIC/Normal.h>
//#include <normal/ssb/common/Operators.h>
//#include <normal/ssb/SSBSchema.h>
//
//using namespace normal::ssb::query1_1;
//using namespace normal::pushdown::aggregate;
//using namespace normal::core::type;
//using namespace normal::expression::gandiva;
//
//std::shared_ptr<OperatorGraph>
//LocalFileSystemQueries::dateScan(const std::string &dataDir, FileType fileType, int numConcurrentUnits, const std::shared_ptr<Normal> &n) {
//
//  auto g = n->createQuery();
//
//  auto dateCacheLoads = common::Operators::makeFileCacheLoadOperators("date", fileType == FileType::CSV ? "date.tbl" : "parquet/date.snappy.parquet",
//																	  SSBSchema::date()->field_names(), dataDir, numConcurrentUnits, g);
//  auto dateScans = common::Operators::makeFileScanOperators("date", fileType == FileType::CSV ? "date.tbl" : "parquet/date.snappy.parquet", fileType,
//															SSBSchema::date()->field_names(), dataDir, numConcurrentUnits, g);
//  auto dateMerges = common::Operators::makeMergeOperators("date", numConcurrentUnits, g);
//  auto collate = common::Operators::makeCollateOperator(g);
//
//  common::Operators::connectHitsToEachLeft(dateCacheLoads, dateMerges);
//  common::Operators::connectMissesToEach(dateCacheLoads, dateScans);
//  common::Operators::connectToEachRight(dateScans, dateMerges);
//
//  common::Operators::connectToOne(dateMerges, collate);
//
//  return g;
//}
//
//std::shared_ptr<OperatorGraph>
//LocalFileSystemQueries::dateFilter(const std::string &dataDir, FileType fileType, short year, int numConcurrentUnits,
//								   const std::shared_ptr<Normal> &n) {
//
//  auto g = n->createQuery();
//
//  auto dateCacheLoads = common::Operators::makeFileCacheLoadOperators("date", fileType == FileType::CSV ? "date.tbl" : "parquet/date.snappy.parquet",
//																	  SSBSchema::date()->field_names(), dataDir, numConcurrentUnits, g);
//  auto dateScans = common::Operators::makeFileScanOperators("date", fileType == FileType::CSV ? "date.tbl" : "parquet/date.snappy.parquet", fileType,
//															SSBSchema::date()->field_names(), dataDir, numConcurrentUnits, g);
//  auto dateMerges = common::Operators::makeMergeOperators("date", numConcurrentUnits, g);
//  auto dateFilters = Operators::makeDateFilterOperators(year, fileType == FileType::CSV, numConcurrentUnits, g);
//  auto collate = common::Operators::makeCollateOperator(g);
//
//  common::Operators::connectHitsToEachLeft(dateCacheLoads, dateMerges);
//  common::Operators::connectMissesToEach(dateCacheLoads, dateScans);
//  common::Operators::connectToEachRight(dateScans, dateMerges);
//
//  common::Operators::connectToEach(dateMerges, dateFilters);
//
//  common::Operators::connectToOne(dateFilters, collate);
//
//  return g;
//}
//
//std::shared_ptr<OperatorGraph>
//LocalFileSystemQueries::lineOrderScan(const std::string &dataDir, FileType fileType, int numConcurrentUnits, const std::shared_ptr<Normal> &n) {
//
//  auto g = n->createQuery();
//
//  auto lineorderCacheLoads = common::Operators::makeFileCacheLoadOperators("lineorder", fileType == FileType::CSV ? "lineorder.tbl" : "parquet/lineorder.snappy.parquet", SSBSchema::lineOrder()->field_names(), dataDir, numConcurrentUnits, g);
//  auto lineorderScans = common::Operators::makeFileScanOperators("lineorder", fileType == FileType::CSV ? "lineorder.tbl" : "parquet/lineorder.snappy.parquet", fileType, SSBSchema::lineOrder()->field_names(), dataDir, numConcurrentUnits, g);
//  auto lineorderMerges = common::Operators::makeMergeOperators("lineorder", numConcurrentUnits, g);
//  auto collate = common::Operators::makeCollateOperator(g);
//
//  common::Operators::connectHitsToEachLeft(lineorderCacheLoads, lineorderMerges);
//  common::Operators::connectMissesToEach(lineorderCacheLoads, lineorderScans);
//  common::Operators::connectToEachRight(lineorderScans, lineorderMerges);
//
//  common::Operators::connectToOne(lineorderMerges, collate);
//
//  return g;
//}
//
//std::shared_ptr<OperatorGraph>
//LocalFileSystemQueries::lineOrderFilter(const std::string &dataDir,
//										FileType fileType,
//										short discount,
//										short quantity,
//										int numConcurrentUnits,
//										const std::shared_ptr<Normal> &n) {
//
//  auto g = n->createQuery();
//
//
//  auto lineOrderCacheLoads = common::Operators::makeFileCacheLoadOperators("lineorder", fileType == FileType::CSV ? "lineorder.tbl" : "parquet/lineorder.snappy.parquet", SSBSchema::lineOrder()->field_names(), dataDir, numConcurrentUnits, g);
//  auto lineOrderScans = common::Operators::makeFileScanOperators("lineorder", fileType == FileType::CSV ? "lineorder.tbl" : "parquet/lineorder.snappy.parquet", fileType, SSBSchema::lineOrder()->field_names(), dataDir, numConcurrentUnits, g);
//  auto lineOrderMerges = common::Operators::makeMergeOperators("lineorder", numConcurrentUnits, g);
//  auto lineOrderFilters = Operators::makeLineOrderFilterOperators(discount, quantity, fileType == FileType::CSV, numConcurrentUnits, g);
//  auto collate = common::Operators::makeCollateOperator(g);
//
//  common::Operators::connectHitsToEachLeft(lineOrderCacheLoads, lineOrderMerges);
//  common::Operators::connectMissesToEach(lineOrderCacheLoads, lineOrderScans);
//  common::Operators::connectToEachRight(lineOrderScans, lineOrderMerges);
//
//  common::Operators::connectToEach(lineOrderMerges, lineOrderFilters);
//
//  common::Operators::connectToOne(lineOrderFilters, collate);
//
//  return g;
//}
//
//std::shared_ptr<OperatorGraph>
//LocalFileSystemQueries::join(const std::string &dataDir,
//							 FileType fileType,
//							 short year,
//							 short discount,
//							 short quantity,
//							 int numConcurrentUnits,
//							 const std::shared_ptr<Normal> &n) {
//
//  auto g = n->createQuery();
//
//  auto dateCacheLoads = common::Operators::makeFileCacheLoadOperators("date", fileType == FileType::CSV ? "date.tbl" : "parquet/date.snappy.parquet",
//																	  SSBSchema::date()->field_names(), dataDir, numConcurrentUnits, g);
//  auto dateScans = common::Operators::makeFileScanOperators("date", fileType == FileType::CSV ? "date.tbl" : "parquet/date.snappy.parquet", fileType,
//															SSBSchema::date()->field_names(), dataDir, numConcurrentUnits, g);
//  auto dateMerges = common::Operators::makeMergeOperators("date", numConcurrentUnits, g);
//  auto lineOrderCacheLoads = common::Operators::makeFileCacheLoadOperators("lineorder", fileType == FileType::CSV ? "lineorder.tbl" : "parquet/lineorder.snappy.parquet", SSBSchema::lineOrder()->field_names(), dataDir, numConcurrentUnits, g);
//  auto lineOrderScans = common::Operators::makeFileScanOperators("lineorder", fileType == FileType::CSV ? "lineorder.tbl" : "parquet/lineorder.snappy.parquet", fileType, SSBSchema::lineOrder()->field_names(), dataDir, numConcurrentUnits, g);
//  auto lineOrderMerges = common::Operators::makeMergeOperators("lineorder", numConcurrentUnits, g);
//  auto dateFilters = Operators::makeDateFilterOperators(year, fileType == FileType::CSV, numConcurrentUnits, g);
//  auto lineOrderFilters = Operators::makeLineOrderFilterOperators(discount, quantity, fileType == FileType::CSV, numConcurrentUnits, g);
//  auto dateShuffles = common::Operators::makeShuffleOperators("date", "d_datekey", numConcurrentUnits, g);
//  auto lineOrderShuffles = common::Operators::makeShuffleOperators("lineorder", "lo_orderdate", numConcurrentUnits, g);
//  auto joinBuild =  common::Operators::makeHashJoinBuildOperators("1", "d_datekey", numConcurrentUnits, g);
//  auto joinProbe = common::Operators::makeHashJoinProbeOperators("1", "d_datekey", "lo_orderdate", numConcurrentUnits, g);
//  auto collate = common::Operators::makeCollateOperator(g);
//
//  common::Operators::connectHitsToEachLeft(dateCacheLoads, dateMerges);
//  common::Operators::connectMissesToEach(dateCacheLoads, dateScans);
//  common::Operators::connectToEachRight(dateScans, dateMerges);
//
//  common::Operators::connectHitsToEachLeft(lineOrderCacheLoads, lineOrderMerges);
//  common::Operators::connectMissesToEach(lineOrderCacheLoads, lineOrderScans);
//  common::Operators::connectToEachRight(lineOrderScans, lineOrderMerges);
//
//  common::Operators::connectToEach(dateMerges, dateFilters);
//  common::Operators::connectToEach(lineOrderMerges, lineOrderFilters);
//
//  common::Operators::connectToEach(dateFilters, dateShuffles);
//  common::Operators::connectToEach(lineOrderFilters, lineOrderShuffles);
//
//  common::Operators::connectToAll(dateShuffles, joinBuild);
//  common::Operators::connectToAll(lineOrderShuffles, joinProbe);
//  common::Operators::connectToEach(joinBuild, joinProbe);
//
//  common::Operators::connectToOne(joinProbe, collate);
//
//  return g;
//}
//
//std::shared_ptr<OperatorGraph>
//LocalFileSystemQueries::full(const std::string &dataDir,
//							 FileType fileType,
//							 short year,
//							 short discount,
//							 short quantity,
//							 int numConcurrentUnits,
//							 const std::shared_ptr<Normal> &n) {
//
//  auto g = n->createQuery();
//
//  auto dateCacheLoads = common::Operators::makeFileCacheLoadOperators("date", fileType == FileType::CSV ? "date.tbl" : "parquet/date.snappy.parquet",
//																	  SSBSchema::date()->field_names(), dataDir, numConcurrentUnits, g);
//  auto dateScans = common::Operators::makeFileScanOperators("date", fileType == FileType::CSV ? "date.tbl" : "parquet/date.snappy.parquet", fileType,
//															SSBSchema::date()->field_names(), dataDir, numConcurrentUnits, g);
//  auto dateMerges = common::Operators::makeMergeOperators("date", numConcurrentUnits, g);
//  auto lineOrderCacheLoads = common::Operators::makeFileCacheLoadOperators("lineorder", fileType == FileType::CSV ? "lineorder.tbl" : "parquet/lineorder.snappy.parquet", SSBSchema::lineOrder()->field_names(), dataDir, numConcurrentUnits, g);
//  auto lineOrderScans = common::Operators::makeFileScanOperators("lineorder", fileType == FileType::CSV ? "lineorder.tbl" : "parquet/lineorder.snappy.parquet", fileType, SSBSchema::lineOrder()->field_names(), dataDir, numConcurrentUnits, g);
//  auto lineOrderMerges = common::Operators::makeMergeOperators("lineorder", numConcurrentUnits, g);
//  auto dateFilters = Operators::makeDateFilterOperators(year, fileType == FileType::CSV, numConcurrentUnits, g);
//  auto lineOrderFilters = Operators::makeLineOrderFilterOperators(discount, quantity, fileType == FileType::CSV, numConcurrentUnits, g);
//  auto dateShuffles = common::Operators::makeShuffleOperators("date", "d_datekey", numConcurrentUnits, g);
//  auto lineOrderShuffles = common::Operators::makeShuffleOperators("lineorder", "lo_orderdate", numConcurrentUnits, g);
//  auto joinBuild =  common::Operators::makeHashJoinBuildOperators("1", "d_datekey", numConcurrentUnits, g);
//  auto joinProbe = common::Operators::makeHashJoinProbeOperators("1", "d_datekey", "lo_orderdate", numConcurrentUnits, g);
//  auto aggregates = Operators::makeAggregateOperators(numConcurrentUnits, g);
//  auto aggregateReduce = Operators::makeAggregateReduceOperator(g);
//  auto collate = common::Operators::makeCollateOperator(g);
//
//  common::Operators::connectHitsToEachLeft(dateCacheLoads, dateMerges);
//  common::Operators::connectMissesToEach(dateCacheLoads, dateScans);
//  common::Operators::connectToEachRight(dateScans, dateMerges);
//
//  common::Operators::connectHitsToEachLeft(lineOrderCacheLoads, lineOrderMerges);
//  common::Operators::connectMissesToEach(lineOrderCacheLoads, lineOrderScans);
//  common::Operators::connectToEachRight(lineOrderScans, lineOrderMerges);
//
//  common::Operators::connectToEach(dateMerges, dateFilters);
//  common::Operators::connectToEach(lineOrderMerges, lineOrderFilters);
//
//  common::Operators::connectToEach(dateFilters, dateShuffles);
//  common::Operators::connectToEach(lineOrderFilters, lineOrderShuffles);
//
//  common::Operators::connectToAll(dateShuffles, joinBuild);
//  common::Operators::connectToAll(lineOrderShuffles, joinProbe);
//  common::Operators::connectToEach(joinBuild, joinProbe);
//
//  common::Operators::connectToEach(joinProbe, aggregates);
//  common::Operators::connectToOne(aggregates, aggregateReduce);
//
//  common::Operators::connectToOne(aggregateReduce, collate);
//
//  return g;
//}
//
//std::shared_ptr<OperatorGraph>
//LocalFileSystemQueries::bloom(const std::string &dataDir,
//							  FileType fileType,
//							 short year,
//							 short discount,
//							 short quantity,
//							 int numConcurrentUnits,
//							 const std::shared_ptr<Normal> &n) {
//
//  auto g = n->createQuery();
//
//  auto dateCacheLoads = common::Operators::makeFileCacheLoadOperators("date", fileType == FileType::CSV ? "date.tbl" : "parquet/date.snappy.parquet",
//																	  SSBSchema::date()->field_names(), dataDir, numConcurrentUnits, g);
//  auto dateScans = common::Operators::makeFileScanOperators("date", fileType == FileType::CSV ? "date.tbl" : "parquet/date.snappy.parquet", fileType,
//															SSBSchema::date()->field_names(), dataDir, numConcurrentUnits, g);
//  auto dateMerges = common::Operators::makeMergeOperators("date", numConcurrentUnits, g);
//  auto dateBloomCreate = common::Operators::makeBloomCreateOperator("d_datekey", "d_datekey", 0.3, g);
//  auto lineOrderCacheLoads = common::Operators::makeFileCacheLoadOperators("lineorder", fileType == FileType::CSV ? "lineorder.tbl" : "parquet/lineorder.snappy.parquet", SSBSchema::lineOrder()->field_names(), dataDir, numConcurrentUnits, g);
//  auto lineOrderScanBloomUses = common::Operators::makeFileScanBloomUseOperators("lo_orderdate", fileType == FileType::CSV ? "lineorder.tbl" : "parquet/lineorder.snappy.parquet", SSBSchema::lineOrder()->field_names(), "lo_orderdate", dataDir, numConcurrentUnits, g);
//  auto lineOrderMerges = common::Operators::makeMergeOperators("lineorder", numConcurrentUnits, g);
//  auto dateFilters = Operators::makeDateFilterOperators(year, fileType == FileType::CSV, numConcurrentUnits, g);
//  auto lineOrderFilters = Operators::makeLineOrderFilterOperators(discount, quantity, fileType == FileType::CSV, numConcurrentUnits, g);
//  auto dateShuffles = common::Operators::makeShuffleOperators("date", "d_datekey", numConcurrentUnits, g);
//  auto lineOrderShuffles = common::Operators::makeShuffleOperators("lineorder", "lo_orderdate", numConcurrentUnits, g);
//  auto joinBuild =  common::Operators::makeHashJoinBuildOperators("1", "d_datekey", numConcurrentUnits, g);
//  auto joinProbe = common::Operators::makeHashJoinProbeOperators("1", "d_datekey", "lo_orderdate", numConcurrentUnits, g);
//  auto aggregates = Operators::makeAggregateOperators(numConcurrentUnits, g);
//  auto aggregateReduce = Operators::makeAggregateReduceOperator(g);
//  auto collate = common::Operators::makeCollateOperator(g);
//
//
//  common::Operators::connectHitsToEach(dateCacheLoads, dateMerges);
//  common::Operators::connectMissesToEach(dateCacheLoads, dateScans);
//  common::Operators::connectToEach(dateScans, dateMerges);
//
//  common::Operators::connectToEach(dateMerges, dateFilters);
//
//  common::Operators::connectToOne(dateFilters, dateBloomCreate);
//  common::Operators::connectToAll(dateBloomCreate, lineOrderScanBloomUses);
//
//  common::Operators::connectHitsToEach(lineOrderCacheLoads, lineOrderMerges);
//  common::Operators::connectMissesToEach(lineOrderCacheLoads, lineOrderScanBloomUses);
//  common::Operators::connectToEach(lineOrderScanBloomUses, lineOrderMerges);
//
//  common::Operators::connectToEach(lineOrderMerges, lineOrderFilters);
//
//  common::Operators::connectToEach(dateFilters, dateShuffles);
//  common::Operators::connectToEach(lineOrderFilters, lineOrderShuffles);
//
//  common::Operators::connectToAll(dateShuffles, joinBuild);
//  common::Operators::connectToAll(lineOrderShuffles, joinProbe);
//  common::Operators::connectToEach(joinBuild, joinProbe);
//
//  common::Operators::connectToEach(joinProbe, aggregates);
//  common::Operators::connectToOne(aggregates, aggregateReduce);
//
//  common::Operators::connectToOne(aggregateReduce, collate);
//
//  return g;
//}
