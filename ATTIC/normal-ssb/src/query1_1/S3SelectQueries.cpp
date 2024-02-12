//
// Created by matt on 26/6/20.
//
//
//#include "normal/ssb/query1_1/S3SelectQueries.h"
//
//#include <normal/ssb/Globals.h>
//#include <normal/ssb/common/Operators.h>
//#include <normal/ssb/query1_1/Operators.h>
//#include <normal/connector/s3/S3Util.h>
//#include <normal/pushdown/Util.h>
//#include <normal/connector/s3/S3SelectPartition.h>
//
//#include <utility>
//#include <normal/connector/MiniCatalogue.h>
//
//using namespace normal::ssb::query1_1;
//using namespace normal::pushdown::aggregate;
//using namespace normal::core::type;
//using namespace normal::expression::gandiva;
//using namespace normal::connector::s3;
//using namespace normal::connector;
//
//namespace {
//
//std::unordered_map<std::string, std::shared_ptr<S3SelectPartition>> discoverPartitions(const std::string &s3Bucket,
//																					   const std::string &s3ObjectPrefix,
//																					   std::vector<std::string> s3Objects,
//																					   const AWSClient &client);
//
//std::unordered_map<std::string, std::shared_ptr<S3SelectPartition>> discoverPartitions(const std::string &s3Bucket,
//																					   const std::string &s3ObjectPrefix,
//																					   std::vector<std::string> s3Objects,
//																					   const AWSClient &client) {
//  auto partitionMap = S3Util::listObjects(s3Bucket, s3ObjectPrefix, std::move(s3Objects), client.defaultS3Client());
//
//  std::unordered_map<std::string, std::shared_ptr<S3SelectPartition>> partitions;
//  SPDLOG_DEBUG("Discovered partitions");
//  for (auto &partition : partitionMap) {
//	SPDLOG_DEBUG("  's3://{}/{}': size: {}", s3Bucket, partition.first, partition.second);
//	partitions.emplace(partition.first,
//					   std::make_shared<S3SelectPartition>(s3Bucket, partition.first, partition.second));
//  }
//  return partitions;
//}
//
//std::unordered_map<std::string, std::shared_ptr<arrow::Schema>> getSchemas() {
//
//  auto miniCatalogue = normal::connector::defaultMiniCatalogue;
//
//  std::unordered_map<std::string, std::shared_ptr<arrow::Schema>> schemas;
//  schemas.emplace("date", miniCatalogue->getSchema("date"));
//  schemas.emplace("part", miniCatalogue->getSchema("part"));
//  schemas.emplace("lineorder", miniCatalogue->getSchema("lineorder"));
//  schemas.emplace("supplier", miniCatalogue->getSchema("supplier"));
//  schemas.emplace("customer", miniCatalogue->getSchema("customer"));
//  return schemas;
//}
//
//}
//
//std::shared_ptr<OperatorGraph>
//S3SelectQueries::dateScanPullUp(const std::string &s3Bucket,
//								const std::string &s3ObjectDir,
//								FileType fileType,
//								int numConcurrentUnits,
//								AWSClient &client,
//								const std::shared_ptr<Normal> &n) {
//
//  auto s3ObjectPrefix = s3ObjectDir + (fileType == FileType::CSV ? "/csv" : "/parquet");
//  auto dateFile = (fileType == FileType::CSV ? "date.tbl" : "date.snappy.parquet");
//
//  auto partitions = discoverPartitions(s3Bucket, s3ObjectPrefix, {dateFile}, client);
//
//  auto g = n->createQuery();
//
//  auto dateScans = common::Operators::makeS3SelectScanPushDownOperators("date",
//																		s3ObjectPrefix + "/" + dateFile,
//																		s3Bucket,
//																		fileType,
//																		getSchemas().at("date")->field_names(),
//																		"select * from s3Object",
//																		true,
//																		numConcurrentUnits,
//																		partitions[s3ObjectPrefix + "/" + dateFile],
//																		getSchemas().at("date"),
//																		client,
//																		g);
//  auto collate = common::Operators::makeCollateOperator(g);
//
//  common::Operators::connectToOne(dateScans, collate);
//
//  return g;
//}
//
//std::shared_ptr<OperatorGraph> S3SelectQueries::dateFilterPullUp(const std::string &s3Bucket,
//																 const std::string &s3ObjectDir,
//																 FileType fileType,
//																 short year,
//																 int numConcurrentUnits,
//																 AWSClient &client,
//																 const std::shared_ptr<Normal> &n) {
//
//  auto s3ObjectPrefix = s3ObjectDir + (fileType == FileType::CSV ? "/csv" : "/parquet");
//  auto dateFile = s3ObjectDir + (fileType == FileType::CSV ? "date.tbl" : "date.snappy.parquet");
//
//  auto partitions = discoverPartitions(s3Bucket, s3ObjectPrefix, {dateFile}, client);
//
//  auto g = n->createQuery();
//
//  auto dateScans = common::Operators::makeS3SelectScanPushDownOperators("date",
//																		dateFile,
//																		s3Bucket,
//																		fileType,
//																		getSchemas().at("date")->field_names(),
//																		"select * from s3Object",
//																		true,
//																		numConcurrentUnits,
//																		partitions[dateFile],
//																		getSchemas().at("date"),
//																		client,
//																		g);
//  auto dateFilters = Operators::makeDateFilterOperators(year, fileType == FileType::CSV, numConcurrentUnits, g);
//  auto collate = common::Operators::makeCollateOperator(g);
//
//  common::Operators::connectToEach(dateScans, dateFilters);
//  common::Operators::connectToOne(dateFilters, collate);
//
//  return g;
//}
//
//std::shared_ptr<OperatorGraph>
//S3SelectQueries::dateFilterHybrid(const std::string &s3Bucket,
//								  const std::string &s3ObjectDir,
//								  FileType fileType,
//								  short year,
//								  int numConcurrentUnits,
//								  AWSClient &client,
//								  const std::shared_ptr<Normal> &n) {
//
//  auto s3ObjectPrefix = s3ObjectDir + (fileType == FileType::CSV ? "/csv" : "/parquet");
//  auto dateFile = s3ObjectDir + (fileType == FileType::CSV ? "date.tbl" : "date.snappy.parquet");
//
//  auto partitions = discoverPartitions(s3Bucket, s3ObjectPrefix, {dateFile}, client);
//
//  auto g = n->createQuery();
//
//  auto dateColumns = std::vector<std::string>{"D_DATEKEY", "D_YEAR"};
//  auto dateCacheLoads = common::Operators::makeCacheLoadOperators("date",
//																  partitions[dateFile],
//																  dateColumns,
//																  numConcurrentUnits,
//																  g);
//  auto dateScans = common::Operators::makeS3SelectScanPushDownOperators("date",
//																		dateFile,
//																		s3Bucket,
//																		fileType,
//																		dateColumns,
//																		fmt::format(
//																			"select {{}} from s3Object where cast(d_year as int) = {}",
//																			year),
//																		false,
//																		numConcurrentUnits,
//																		partitions[dateFile],
//																		getSchemas().at("date"),
//																		client,
//																		g);
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
//S3SelectQueries::lineOrderScanPullUp(const std::string &s3Bucket,
//									 const std::string &s3ObjectDir,
//									 FileType fileType,
//									 int numConcurrentUnits,
//									 AWSClient &client,
//									 const std::shared_ptr<Normal> &n) {
//
//  auto s3ObjectPrefix = s3ObjectDir + (fileType == FileType::CSV ? "/csv" : "/parquet");
//  auto lineOrderFile =
//	  s3ObjectDir + (fileType == FileType::CSV ? "lineorder.tbl" : "lineorder.snappy.parquet");
//
//  auto partitions = discoverPartitions(s3Bucket, s3ObjectPrefix, {lineOrderFile}, client);
//
//  auto g = n->createQuery();
//
//  auto lineOrderScans = common::Operators::makeS3SelectScanPushDownOperators("lineorder",
//																			 lineOrderFile,
//																			 s3Bucket,
//																			 fileType,
//																			 getSchemas().at("lineorder")->field_names(),
//																			 "select * from s3Object",
//																			 true,
//																			 numConcurrentUnits,
//																			 partitions[lineOrderFile],
//																			 getSchemas().at("lineorder"),
//																			 client,
//																			 g);
//  auto collate = common::Operators::makeCollateOperator(g);
//
//  common::Operators::connectToOne(lineOrderScans, collate);
//
//  return g;
//}
//
//std::shared_ptr<OperatorGraph>
//S3SelectQueries::lineOrderFilterPullUp(const std::string &s3Bucket,
//									   const std::string &s3ObjectDir,
//									   FileType fileType,
//									   short discount,
//									   short quantity,
//									   int numConcurrentUnits,
//									   AWSClient &client,
//									   const std::shared_ptr<Normal> &n) {
//
//  auto s3ObjectPrefix = s3ObjectDir + (fileType == FileType::CSV ? "/csv" : "/parquet");
//  auto lineOrderFile =
//	  s3ObjectDir + (fileType == FileType::CSV ? "lineorder.tbl" : "lineorder.snappy.parquet");
//
//  auto partitions = discoverPartitions(s3Bucket, s3ObjectPrefix, {lineOrderFile}, client);
//
//  auto g = n->createQuery();
//
//  auto lineOrderScans = common::Operators::makeS3SelectScanPushDownOperators("lineorder",
//																			 lineOrderFile,
//																			 s3Bucket,
//																			 fileType,
//																			 getSchemas().at("lineorder")->field_names(),
//																			 "select * from s3Object",
//																			 true,
//																			 numConcurrentUnits,
//																			 partitions[lineOrderFile],
//																			 getSchemas().at("lineorder"),
//																			 client,
//																			 g);
//  auto lineOrderFilters = Operators::makeLineOrderFilterOperators(discount, quantity, fileType == FileType::CSV, numConcurrentUnits, g);
//  auto collate = common::Operators::makeCollateOperator(g);
//
//  common::Operators::connectToEach(lineOrderScans, lineOrderFilters);
//  common::Operators::connectToOne(lineOrderFilters, collate);
//
//  return g;
//}
//
//std::shared_ptr<OperatorGraph> S3SelectQueries::joinPullUp(const std::string &s3Bucket,
//														   const std::string &s3ObjectDir,
//														   FileType fileType,
//														   short year,
//														   short discount,
//														   short quantity,
//														   int numConcurrentUnits,
//														   AWSClient &client,
//														   const std::shared_ptr<Normal> &n) {
//
//  auto s3ObjectPrefix = s3ObjectDir + (fileType == FileType::CSV ? "/csv" : "/parquet");
//  auto dateFile = s3ObjectDir + (fileType == FileType::CSV ? "date.tbl" : "date.snappy.parquet");
//  auto lineOrderFile =
//	  s3ObjectDir + (fileType == FileType::CSV ? "lineorder.tbl" : "lineorder.snappy.parquet");
//
//  auto partitions = discoverPartitions(s3Bucket, s3ObjectPrefix, {dateFile, lineOrderFile}, client);
//
//  auto g = n->createQuery();
//
//  auto dateScans = common::Operators::makeS3SelectScanPushDownOperators("date",
//																		dateFile,
//																		s3Bucket,
//																		fileType,
//																		getSchemas().at("date")->field_names(),
//																		"select * from s3Object",
//																		true,
//																		numConcurrentUnits,
//																		partitions[dateFile],
//																		getSchemas().at("date"),
//																		client,
//																		g);
//  auto lineOrderScans = common::Operators::makeS3SelectScanPushDownOperators("lineorder",
//																			 lineOrderFile,
//																			 s3Bucket,
//																			 fileType,
//																			 getSchemas().at("lineorder")->field_names(),
//																			 "select * from s3Object",
//																			 true,
//																			 numConcurrentUnits,
//																			 partitions[lineOrderFile],
//																			 getSchemas().at("lineorder"),
//																			 client,
//																			 g);
//  auto dateFilters = Operators::makeDateFilterOperators(year, fileType == FileType::CSV, numConcurrentUnits, g);
//  auto lineOrderFilters =
//	  Operators::makeLineOrderFilterOperators(discount, quantity, fileType == FileType::CSV, numConcurrentUnits, g);
//  auto dateShuffles = common::Operators::makeShuffleOperators("date", "d_datekey", numConcurrentUnits, g);
//  auto lineOrderShuffles = common::Operators::makeShuffleOperators("lineorder", "lo_orderdate", numConcurrentUnits, g);
//  auto joinBuild = common::Operators::makeHashJoinBuildOperators("d_datekey", "d_datekey", numConcurrentUnits, g);
//  auto joinProbe =
//	  common::Operators::makeHashJoinProbeOperators("lo_orderdate", "d_datekey", "lo_orderdate", numConcurrentUnits, g);
//  auto collate = common::Operators::makeCollateOperator(g);
//
//  common::Operators::connectToEach(dateScans, dateFilters);
//  common::Operators::connectToEach(lineOrderScans, lineOrderFilters);
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
//std::shared_ptr<OperatorGraph> S3SelectQueries::fullPullUp(const std::string &s3Bucket,
//														   const std::string &s3ObjectDir,
//														   FileType fileType,
//														   short year,
//														   short discount,
//														   short quantity,
//														   int numConcurrentUnits,
//														   AWSClient &client,
//														   const std::shared_ptr<Normal> &n) {
//
//  auto s3ObjectPrefix = s3ObjectDir + (fileType == FileType::CSV ? "/csv" : "/parquet");
//  auto dateFile = s3ObjectDir + (fileType == FileType::CSV ? "date.tbl" : "date.snappy.parquet");
//  auto lineOrderFile =
//	  s3ObjectDir + (fileType == FileType::CSV ? "lineorder.tbl" : "lineorder.snappy.parquet");
//  auto partitions = discoverPartitions(s3Bucket, s3ObjectPrefix, {dateFile, lineOrderFile}, client);
//
//  auto g = n->createQuery();
//
//  auto dateScans = common::Operators::makeS3SelectScanPushDownOperators("date",
//																		dateFile,
//																		s3Bucket,
//																		fileType,
//																		getSchemas().at("date")->field_names(),
//																		"select * from s3Object",
//																		true,
//																		numConcurrentUnits,
//																		partitions[dateFile],
//																		getSchemas().at("date"),
//																		client,
//																		g);
//  auto lineOrderScans = common::Operators::makeS3SelectScanPushDownOperators("lineorder",
//																			 lineOrderFile,
//																			 s3Bucket,
//																			 fileType,
//																			 getSchemas().at("lineorder")->field_names(),
//																			 "select * from s3Object",
//																			 true,
//																			 numConcurrentUnits,
//																			 partitions[lineOrderFile],
//																			 getSchemas().at("lineorder"),
//																			 client,
//																			 g);
//  auto dateFilters = Operators::makeDateFilterOperators(year, fileType == FileType::CSV, numConcurrentUnits, g);
//  auto lineOrderFilters =
//	  Operators::makeLineOrderFilterOperators(discount, quantity, fileType == FileType::CSV, numConcurrentUnits, g);
//  auto dateShuffles = common::Operators::makeShuffleOperators("date", "d_datekey", numConcurrentUnits, g);
//  auto lineOrderShuffles = common::Operators::makeShuffleOperators("lineorder", "lo_orderdate", numConcurrentUnits, g);
//  auto joinBuild = common::Operators::makeHashJoinBuildOperators("d_datekey", "d_datekey", numConcurrentUnits, g);
//  auto joinProbe =
//	  common::Operators::makeHashJoinProbeOperators("lo_orderdate", "d_datekey", "lo_orderdate", numConcurrentUnits, g);
//  auto aggregates = Operators::makeAggregateOperators(numConcurrentUnits, g);
//  auto aggregateReduce = Operators::makeAggregateReduceOperator(g);
//  auto collate = common::Operators::makeCollateOperator(g);
//
//  common::Operators::connectToEach(dateScans, dateFilters);
//  common::Operators::connectToEach(lineOrderScans, lineOrderFilters);
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
//std::shared_ptr<OperatorGraph> S3SelectQueries::fullPushDown(const std::string &s3Bucket,
//															 const std::string &s3ObjectDir,
//															 FileType fileType,
//															 short year,
//															 short discount,
//															 short quantity,
//															 int numConcurrentUnits,
//															 AWSClient &client,
//															 const std::shared_ptr<Normal> &n) {
//
//  auto s3ObjectPrefix = s3ObjectDir + (fileType == FileType::CSV ? "/csv" : "/parquet");
//  auto dateFile = s3ObjectDir + (fileType == FileType::CSV ? "date.tbl" : "date.snappy.parquet");
//  auto lineOrderFile =
//	  s3ObjectDir + (fileType == FileType::CSV ? "/csv/lineorder.tbl" : "/parquet/lineorder.snappy.parquet");
//
//  auto partitions = discoverPartitions(s3Bucket, s3ObjectPrefix, {dateFile, lineOrderFile}, client);
//
//  auto g = n->createQuery();
//
//  auto dateColumns = std::vector<std::string>{"D_DATEKEY", "D_YEAR"};
//  auto dateScans = common::Operators::makeS3SelectScanPushDownOperators("date",
//																		dateFile,
//																		s3Bucket,
//																		fileType,
//																		dateColumns,
//																		fmt::format(
//																			"select {{}} from s3Object where cast(d_year as int) = {}",
//																			year),
//																		true,
//																		numConcurrentUnits,
//																		partitions[dateFile],
//																		getSchemas().at("date"),
//																		client, g);
//  auto lineOrderColumns = std::vector<std::string>{"LO_ORDERDATE",
//												   "LO_QUANTITY",
//												   "LO_EXTENDEDPRICE",
//												   "LO_DISCOUNT",
//												   "LO_REVENUE"};
//  auto lineOrderScans = common::Operators::makeS3SelectScanPushDownOperators("lineorder",
//																			 lineOrderFile,
//																			 s3Bucket,
//																			 fileType,
//																			 lineOrderColumns,
//																			 fmt::format(
//																				 "select {{}} from s3Object where cast(lo_discount as int) between {} and {} and cast(lo_quantity as int) < {}",
//																				 discount - 1,
//																				 discount + 1,
//																				 quantity),
//																			 true,
//																			 numConcurrentUnits,
//																			 partitions[lineOrderFile],
//																			 getSchemas().at("lineorder"),
//																			 client,
//																			 g);
//  auto dateFilters = Operators::makeDateFilterOperators(year, fileType == FileType::CSV, numConcurrentUnits, g);
//  auto lineOrderFilters = Operators::makeLineOrderFilterOperators(discount,
//																  quantity,
//																  fileType == FileType::CSV,
//																  numConcurrentUnits,
//																  g);
//  auto dateShuffles = common::Operators::makeShuffleOperators("date", "d_datekey", numConcurrentUnits, g);
//  auto lineOrderShuffles = common::Operators::makeShuffleOperators("lineorder", "lo_orderdate", numConcurrentUnits, g);
//  auto joinBuild = common::Operators::makeHashJoinBuildOperators("1", "d_datekey", numConcurrentUnits, g);
//  auto joinProbe = common::Operators::makeHashJoinProbeOperators("1",
//																 "d_datekey",
//																 "lo_orderdate",
//																 numConcurrentUnits,
//																 g);
//  auto aggregates = Operators::makeAggregateOperators(numConcurrentUnits, g);
//  auto aggregateReduce = Operators::makeAggregateReduceOperator(g);
//  auto collate = common::Operators::makeCollateOperator(g);
//
//  common::Operators::connectToEach(dateScans, dateFilters);
//  common::Operators::connectToEach(lineOrderScans, lineOrderFilters);
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
//std::shared_ptr<OperatorGraph> S3SelectQueries::fullHybrid(const std::string &s3Bucket,
//														   const std::string &s3ObjectDir,
//														   FileType fileType,
//														   short year,
//														   short discount,
//														   short quantity,
//														   int numConcurrentUnits,
//														   AWSClient &client,
//														   const std::shared_ptr<Normal> &n) {
//
//  auto s3ObjectPrefix = s3ObjectDir + (fileType == FileType::CSV ? "/csv" : "/parquet");
//  auto dateFile = s3ObjectDir + (fileType == FileType::CSV ? "date.tbl" : "date.snappy.parquet");
//  auto lineOrderFile =
//	  s3ObjectDir + (fileType == FileType::CSV ? "/csv/lineorder.tbl" : "/parquet/lineorder.snappy.parquet");
//  auto partitions = discoverPartitions(s3Bucket, s3ObjectPrefix, {dateFile, lineOrderFile}, client);
//
//  auto g = n->createQuery();
//
//  auto dateCacheLoads = common::Operators::makeCacheLoadOperators("date",
//																  partitions[dateFile],
//																  getSchemas().at("date")->field_names(),
//																  numConcurrentUnits,
//																  g);
//  auto dateColumns = std::vector<std::string>{"D_DATEKEY", "D_YEAR"};
//  auto dateScans = common::Operators::makeS3SelectScanPushDownOperators("date",
//																		dateFile,
//																		s3Bucket,
//																		fileType,
//																		dateColumns,
//																		fmt::format(
//																			"select {{}} from s3Object where cast(d_year as int) = {}",
//																			year),
//																		false,
//																		numConcurrentUnits,
//																		partitions[dateFile],
//																		getSchemas().at("date"),
//																		client, g);
//  auto dateMerges = common::Operators::makeMergeOperators("date", numConcurrentUnits, g);
//  auto lineOrderCacheLoads = common::Operators::makeCacheLoadOperators("lineorder",
//																	   partitions[dateFile],
//																	   getSchemas().at("lineorder")->field_names(),
//																	   numConcurrentUnits,
//																	   g);
//  auto lineOrderColumns = std::vector<std::string>{"LO_ORDERDATE",
//												   "LO_QUANTITY",
//												   "LO_EXTENDEDPRICE",
//												   "LO_DISCOUNT",
//												   "LO_REVENUE"};
//  auto lineOrderScans = common::Operators::makeS3SelectScanPushDownOperators("lineorder",
//																			 lineOrderFile,
//																			 s3Bucket,
//																			 fileType,
//																			 lineOrderColumns,
//																			 fmt::format(
//																				 "select {{}} from s3Object where cast(lo_discount as int) between {} and {} and cast(lo_quantity as int) < {}",
//																				 discount - 1,
//																				 discount + 1,
//																				 quantity),
//																			 false,
//																			 numConcurrentUnits,
//																			 partitions[lineOrderFile],
//																			 getSchemas().at("lineorder"),
//																			 client,
//																			 g);
//  auto lineOrderMerges = common::Operators::makeMergeOperators("lineorder", numConcurrentUnits, g);
//  auto dateShuffles = common::Operators::makeShuffleOperators("date", "d_datekey", numConcurrentUnits, g);
//  auto lineOrderShuffles = common::Operators::makeShuffleOperators("lineorder", "lo_orderdate", numConcurrentUnits, g);
//  auto joinBuild = common::Operators::makeHashJoinBuildOperators("d_datekey", "d_datekey", numConcurrentUnits, g);
//  auto joinProbe =
//	  common::Operators::makeHashJoinProbeOperators("lo_orderdate", "d_datekey", "lo_orderdate", numConcurrentUnits, g);
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
//  common::Operators::connectToEach(dateMerges, dateShuffles);
//  common::Operators::connectToEach(lineOrderMerges, lineOrderShuffles);
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
