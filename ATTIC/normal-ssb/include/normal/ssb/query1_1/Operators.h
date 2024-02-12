//
// Created by matt on 26/6/20.
//

#ifndef NORMAL_NORMAL_SSB_INCLUDE_NORMAL_SSB_QUERY_1_1_OPERATORS_H
#define NORMAL_NORMAL_SSB_INCLUDE_NORMAL_SSB_QUERY_1_1_OPERATORS_H
//
//#include <vector>
//#include <memory>
//
//#include <normal/pushdown/AWSClient.h>
//#include <normal/pushdown/file/FileScan.h>
//#include <normal/pushdown/filter/Filter.h>
//#include <normal/pushdown/collate/Collate.h>
//#include <normal/pushdown/join/HashJoinBuild.h>
//#include <normal/pushdown/join/HashJoinProbe.h>
//#include <normal/pushdown/aggregate/Aggregate.h>
//#include <normal/pushdown/s3/S3Select.h>
//#include <normal/pushdown/s3/S3Get.h>
//#include <normal/pushdown/shuffle/Shuffle.h>
//#include <normal/core/graph/OperatorGraph.h>
//#include <normal/pushdown/cache/CacheLoad.h>
//#include <normal/pushdown/merge/Merge.h>
//#include <normal/pushdown/bloomjoin/BloomCreateOperator.h>
//#include <normal/pushdown/bloomjoin/FileScanBloomUseOperator.h>
//
//using namespace normal::core::graph;
//using namespace normal::pushdown;
//using namespace normal::pushdown::aggregate;
//using namespace normal::pushdown::filter;
//using namespace normal::pushdown::join;
//using namespace normal::pushdown::shuffle;
//using namespace normal::pushdown::cache;
//using namespace normal::pushdown::merge;
//using namespace normal::pushdown::bloomjoin;
//
//namespace normal::ssb::query1_1 {
//
///**
// * Normal operator factories for SSB query 1.1
// */
//class Operators {
//
//public:
//
//  static std::vector<std::shared_ptr<CacheLoad>>
//  makeDateS3SelectCacheLoadOperators(const std::string &s3ObjectDir,
//									 const std::string &s3Bucket,
//									 int numConcurrentUnits,
//									 std::unordered_map<std::string, long> partitionMap,
//									 AWSClient &client,
//									 const std::shared_ptr<OperatorGraph> &g);
//
////  static std::vector<std::shared_ptr<CacheLoad>>
////  makeDateFileCacheLoadOperators(const std::string &dataDir,
////								 int numConcurrentUnits,
////								 const std::shared_ptr<OperatorGraph> &g);
//
////  static std::vector<std::shared_ptr<FileScan>>
////  makeDateFileScanOperators(const std::string &dataDir,
////							int numConcurrentUnits,
////							const std::shared_ptr<OperatorGraph> &g);
//
////  static std::vector<std::shared_ptr<Merge>>
////  makeDateMergeOperators(int numConcurrentUnits,
////						 const std::shared_ptr<OperatorGraph> &g);
//
//  static std::vector<std::shared_ptr<S3SelectScan>>
//  makeDateS3SelectScanOperators(const std::string &s3ObjectDir,
//								const std::string &s3Bucket,
//								int numConcurrentUnits,
//								std::unordered_map<std::string, long> partitionMap,
//								AWSClient &client,
//								const std::shared_ptr<OperatorGraph> &g);
//
//  static std::vector<std::shared_ptr<S3SelectScan>>
//  makeDateS3SelectScanPushDownOperators(const std::string &s3ObjectDir,
//										const std::string &s3Bucket,
//										short year,
//										int numConcurrentUnits,
//										std::unordered_map<std::string, long> partitionMap,
//										AWSClient &client,
//										const std::shared_ptr<OperatorGraph> &g);
//
////  static std::vector<std::shared_ptr<CacheLoad>>
////  makeLineOrderFileCacheLoadOperators(const std::string &dataDir,
////									  int numConcurrentUnits,
////									  const std::shared_ptr<OperatorGraph> &g);
//
////  static std::vector<std::shared_ptr<FileScan>>
////  makeLineOrderFileScanOperators(const std::string &dataDir,
////								 int numConcurrentUnits,
////								 const std::shared_ptr<OperatorGraph> &g);
//
////  static std::vector<std::shared_ptr<Merge>>
////  makeLineOrderMergeOperators(int numConcurrentUnits,
////							  const std::shared_ptr<OperatorGraph> &g);
//
//  static std::vector<std::shared_ptr<S3SelectScan>>
//  makeLineOrderS3SelectScanOperators(const std::string &s3ObjectDir,
//									 const std::string &s3Bucket,
//									 int numConcurrentUnits,
//									 std::unordered_map<std::string, long> partitionMap,
//									 AWSClient &client,
//									 const std::shared_ptr<OperatorGraph> &g);
//
//  static std::vector<std::shared_ptr<S3SelectScan>>
//  makeLineOrderS3SelectScanPushdownOperators(const std::string &s3ObjectDir,
//											 const std::string &s3Bucket,
//											 short discount, short quantity,
//											 int numConcurrentUnits,
//											 std::unordered_map<std::string, long> partitionMap,
//											 AWSClient &client,
//											 const std::shared_ptr<OperatorGraph> &g);
//
//  static std::vector<std::shared_ptr<normal::pushdown::filter::Filter>>
//  makeDateFilterOperators(short year, bool castValues, int numConcurrentUnits, const std::shared_ptr<OperatorGraph> &g);
//
//  static std::vector<std::shared_ptr<normal::pushdown::filter::Filter>>
//  makeLineOrderFilterOperators(short discount,
//							   short quantity,
//							   bool castValues,
//							   int numConcurrentUnits,
//							   const std::shared_ptr<OperatorGraph> &g);
//
////  static std::vector<std::shared_ptr<Shuffle>>
////  makeDateShuffleOperators(int numConcurrentUnits, const std::shared_ptr<OperatorGraph> &g);
//
////  static std::vector<std::shared_ptr<Shuffle>>
////  makeLineOrderShuffleOperators(int numConcurrentUnits, bool castValues, const std::shared_ptr<OperatorGraph> &g);
//
////  static std::vector<std::shared_ptr<HashJoinBuild>>
////  makeHashJoinBuildOperators(int numConcurrentUnits, const std::shared_ptr<OperatorGraph> &g);
//
////  static std::vector<std::shared_ptr<HashJoinProbe>>
////  makeHashJoinProbeOperators(int numConcurrentUnits, const std::shared_ptr<OperatorGraph> &g);
//
//  static std::vector<std::shared_ptr<Aggregate>>
//  makeAggregateOperators(int numConcurrentUnits, const std::shared_ptr<OperatorGraph> &g);
//
//  static std::shared_ptr<Aggregate> makeAggregateReduceOperator(const std::shared_ptr<OperatorGraph> &g);
//
////  static std::shared_ptr<Collate> makeCollateOperator(const std::shared_ptr<OperatorGraph> &g);
//
//  static std::shared_ptr<BloomCreateOperator>
//  makeDateBloomCreateOperators(const std::shared_ptr<OperatorGraph>& g);
//
//  static std::vector<std::shared_ptr<FileScanBloomUseOperator>>
//  makeLineOrderFileScanBloomUseOperators(const std::string &dataDir, int numConcurrentUnits, const std::shared_ptr<OperatorGraph>& g);
//
//};
//
//}

#endif //NORMAL_NORMAL_SSB_INCLUDE_NORMAL_SSB_QUERY_1_1_OPERATORS_H
