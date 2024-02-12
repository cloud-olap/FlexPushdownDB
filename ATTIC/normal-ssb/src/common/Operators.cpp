//
// Created by matt on 10/8/20.
//
//
//#include "normal/ssb/common/Operators.h"
//
//#include <experimental/filesystem>
//
//#include <normal/ssb/Globals.h>
//#include <normal/core/graph/OperatorGraph.h>
//#include <normal/pushdown/Util.h>
//
//#include <normal/connector/local-fs/LocalFilePartition.h>
//
//using namespace normal::ssb::common;
//using namespace std::experimental;
//using namespace normal::pushdown::aggregate;
//using namespace normal::core::type;
//using namespace normal::core::graph;
//using namespace normal::expression::gandiva;
//
//std::vector<std::shared_ptr<CacheLoad>>
//Operators::makeFileCacheLoadOperators(const std::string &namePrefix,
//									  const std::string &filename,
//									  const std::vector<std::string> &columns,
//									  const std::string &dataDir,
//									  int numConcurrentUnits,
//									  const std::shared_ptr<OperatorGraph> &g) {
//
//  auto file = filesystem::absolute(dataDir + "/" + filename);
//  auto numBytesFile = filesystem::file_size(file);
//
//  std::vector<std::shared_ptr<CacheLoad>> os;
//  auto scanRanges = Util::ranges<int>(0, numBytesFile, numConcurrentUnits);
//  for (int u = 0; u < numConcurrentUnits; ++u) {
//	std::shared_ptr<Partition> partition = std::make_shared<LocalFilePartition>(file);
//	auto o = CacheLoad::make(fmt::format("/query-{}/{}-cache-load-{}", g->getId(), namePrefix, u),
//							 columns,
//							 std::vector<std::string>(),
//							 partition,
//							 scanRanges[u].first,
//							 scanRanges[u].second,
//							 true);
//	os.push_back(o);
//	g->put(o);
//  }
//
//  return os;
//}
//
//std::vector<std::shared_ptr<CacheLoad>>
//Operators::makeCacheLoadOperators(const std::string &namePrefix,
//								  const std::shared_ptr<Partition> &partition,
//								  const std::vector<std::string> &columns,
//								  int numConcurrentUnits,
//								  const std::shared_ptr<OperatorGraph> &g) {
//
//  std::vector<std::shared_ptr<CacheLoad>> os;
//  auto scanRanges = Util::ranges(0l, partition->getNumBytes(), numConcurrentUnits);
//  for (int u = 0; u < numConcurrentUnits; ++u) {
//	auto o = CacheLoad::make(fmt::format("/query-{}/{}-cache-load-{}", g->getId(), namePrefix, u),
//							 columns,
//							 std::vector<std::string>(),
//							 partition,
//							 scanRanges[u].first,
//							 scanRanges[u].second,
//							 true);
//	os.push_back(o);
//	g->put(o);
//  }
//
//  return os;
//}
//
////std::vector<std::shared_ptr<FileScan>>
////Operators::makeFileScanOperators(const std::string &namePrefix,
////								 const std::string &filename,
////								 const std::vector<std::string> &columns,
////								 const std::string &dataDir,
////								 int numConcurrentUnits,
////								 const std::shared_ptr<OperatorGraph> &g) {
////
////  auto file = filesystem::absolute(dataDir + "/" + filename);
////  auto numBytesFile = filesystem::file_size(file);
////
////  std::vector<std::shared_ptr<FileScan>> os;
////  auto scanRanges = Util::ranges<int>(0, numBytesFile, numConcurrentUnits);
////  for (int u = 0; u < numConcurrentUnits; ++u) {
////	auto o = FileScan::make(fmt::format("/query-{}/{}-scan-{}", g->getId(), namePrefix, u),
////							file,
////							columns,
////							scanRanges[u].first,
////							scanRanges[u].second,
////							g->getId());
////	os.push_back(o);
////	g->put(o);
////  }
////
////  return os;
////}
//
//std::vector<std::shared_ptr<FileScan>>
//Operators::makeFileScanOperators(const std::string &namePrefix,
//								 const std::string &filename,
//								 FileType fileType,
//								 const std::vector<std::string> &columns,
//								 const std::string &dataDir,
//								 int numConcurrentUnits,
//								 const std::shared_ptr<OperatorGraph> &g) {
//
//  auto file = filesystem::absolute(dataDir + "/" + filename);
//  auto numBytesFile = filesystem::file_size(file);
//
//  std::vector<std::shared_ptr<FileScan>> os;
//  auto scanRanges = Util::ranges<int>(0, numBytesFile, numConcurrentUnits);
//  for (int u = 0; u < numConcurrentUnits; ++u) {
//	auto o = FileScan::make(fmt::format("/query-{}/{}-scan-{}", g->getId(), namePrefix, u),
//							file,
//							fileType,
//							columns,
//							scanRanges[u].first,
//							scanRanges[u].second,
//							g->getId());
//	os.push_back(o);
//	g->put(o);
//  }
//
//  return os;
//}
//
//std::vector<std::shared_ptr<S3SelectScan2>>
//Operators::makeS3SelectScanPushDownOperators(const std::string &namePrefix,
//											 const std::string &s3Object,
//											 const std::string &s3Bucket,
//											 FileType fileType,
//											 const std::vector<std::string> &columns,
//											 const std::string &sql,
//											 bool scanOnStart,
//											 int numConcurrentUnits,
//											 const std::shared_ptr<S3SelectPartition>& partition,
//											 const std::shared_ptr<arrow::Schema>& schema,
//											 AWSClient &client, const std::shared_ptr<OperatorGraph> &g) {
//
//  std::vector<std::shared_ptr<S3SelectScan2>> os;
//  auto scanRanges = Util::ranges<long>(0, partition->getNumBytes(), numConcurrentUnits);
//  for (int u = 0; u < numConcurrentUnits; ++u) {
//	auto o = S3SelectScan2::make(
//		fmt::format("/query-{}/{}-s3-select-scan-{}", g->getId(), namePrefix, u),
//		s3Bucket,
//		s3Object,
//		sql,
//		scanRanges[u].first,
//		scanRanges[u].second,
//		fileType,
//		columns,
//		schema,
//		S3SelectCSVParseOptions(",", "\n"),
//		client.defaultS3Client(),
//		scanOnStart);
//	os.push_back(o);
//	g->put(o);
//  }
//
//  return os;
//}
//
//std::vector<std::shared_ptr<Merge>>
//Operators::makeMergeOperators(const std::string &namePrefix, int numConcurrentUnits,
//							  const std::shared_ptr<OperatorGraph> &g) {
//
//  std::vector<std::shared_ptr<Merge>> os;
//  for (int u = 0; u < numConcurrentUnits; ++u) {
//	auto o = Merge::make(fmt::format("/query-{}/{}-merge-{}", g->getId(), namePrefix, u));
//	os.push_back(o);
//	g->put(o);
//  }
//
//  return os;
//}
//
//std::shared_ptr<BloomCreateOperator>
//Operators::makeBloomCreateOperator(const std::string &namePrefix,
//								   const std::string &columnName,
//								   double desiredFalsePositiveRate,
//								   const std::shared_ptr<OperatorGraph> &g) {
//
//  auto o = BloomCreateOperator::make(fmt::format("/query-{}/{}-bloom-create", g->getId(), namePrefix),
//									 columnName,
//									 desiredFalsePositiveRate,
//									 {});
//
//  g->put(o);
//  return o;
//}
//
//std::vector<std::shared_ptr<FileScanBloomUseOperator>>
//Operators::makeFileScanBloomUseOperators(const std::string &namePrefix,
//										 const std::string &filename,
//										 const std::vector<std::string> &columns,
//										 const std::string &columnName,
//										 const std::string &dataDir,
//										 int numConcurrentUnits,
//										 const std::shared_ptr<OperatorGraph> &g) {
//
//  auto file = filesystem::absolute(dataDir + "/" + filename);
//  auto numBytesFile = filesystem::file_size(file);
//
//  std::vector<std::shared_ptr<FileScanBloomUseOperator>> os;
//  auto scanRanges = Util::ranges<int>(0, numBytesFile, numConcurrentUnits);
//  for (int u = 0; u < numConcurrentUnits; ++u) {
//	auto o = FileScanBloomUseOperator::make(fmt::format("/query-{}/{}-scan-bloom-use-{}", g->getId(), namePrefix, u),
//											file,
//											columns,
//											scanRanges[u].first,
//											scanRanges[u].second,
//											columnName);
//	os.push_back(o);
//	g->put(o);
//  }
//
//  return os;
//}
//
//std::vector<std::shared_ptr<Shuffle>>
//Operators::makeShuffleOperators(const std::string &namePrefix,
//								const std::string &columnName,
//								int numConcurrentUnits,
//								const std::shared_ptr<OperatorGraph> &g) {
//
//  std::vector<std::shared_ptr<Shuffle>> os;
//  for (int u = 0; u < numConcurrentUnits; ++u) {
//	auto o = Shuffle::make(fmt::format("/query-{}/{}-shuffle-{}", g->getId(), namePrefix, u), columnName);
//	os.push_back(o);
//	g->put(o);
//  }
//
//  return os;
//}
//
//std::vector<std::shared_ptr<HashJoinBuild>>
//Operators::makeHashJoinBuildOperators(const std::string &namePrefix,
//									  const std::string &columnName,
//									  int numConcurrentUnits,
//									  const std::shared_ptr<OperatorGraph> &g) {
//  std::vector<std::shared_ptr<HashJoinBuild>> os;
//  os.reserve(numConcurrentUnits);
//  for (int u = 0; u < numConcurrentUnits; ++u) {
//	auto o = HashJoinBuild::create(fmt::format("/query-{}/{}-join-build-{}", g->getId(), namePrefix, u),
//								   columnName);
//	os.push_back(o);
//	g->put(o);
//  }
//  return os;
//}
//
//std::vector<std::shared_ptr<HashJoinProbe>>
//Operators::makeHashJoinProbeOperators(const std::string &namePrefix,
//									  const std::string &leftColumnName,
//									  const std::string &rightColumnName,
//									  int numConcurrentUnits,
//									  const std::shared_ptr<OperatorGraph> &g) {
//  std::vector<std::shared_ptr<HashJoinProbe>> os;
//  os.reserve(numConcurrentUnits);
//  for (int u = 0; u < numConcurrentUnits; ++u) {
//	auto o = std::make_shared<HashJoinProbe>(fmt::format("/query-{}/{}-join-probe-{}",
//														 g->getId(),
//														 namePrefix,
//														 u),
//											 JoinPredicate::create(leftColumnName, rightColumnName),
//											 // TODO: need to set properly
//                       std::set<std::string>());
//	os.push_back(o);
//	g->put(o);
//  }
//  return os;
//}
//
//std::shared_ptr<Collate> Operators::makeCollateOperator(const std::shared_ptr<OperatorGraph> &g) {
//  auto o = std::make_shared<Collate>(fmt::format("/query-{}/collate", g->getId()), g->getId());
//  g->put(o);
//  return o;
//}
