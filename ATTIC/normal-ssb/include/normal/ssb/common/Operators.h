//
// Created by matt on 10/8/20.
//

#ifndef NORMAL_NORMAL_SSB_INCLUDE_NORMAL_SSB_COMMON_OPERATORS_H
#define NORMAL_NORMAL_SSB_INCLUDE_NORMAL_SSB_COMMON_OPERATORS_H
//
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
//#include <normal/pushdown/s3/S3SelectScan2.h>
//#include <normal/pushdown/shuffle/Shuffle.h>
//#include <normal/core/graph/OperatorGraph.h>
//#include <normal/pushdown/cache/CacheLoad.h>
//#include <normal/pushdown/merge/Merge.h>
//#include <normal/pushdown/bloomjoin/BloomCreateOperator.h>
//#include <normal/pushdown/bloomjoin/FileScanBloomUseOperator.h>
//#include <normal/connector/s3/S3SelectPartition.h>
//
//using namespace normal::core::graph;
//using namespace normal::pushdown;
//using namespace normal::pushdown::filter;
//using namespace normal::pushdown::join;
//using namespace normal::pushdown::shuffle;
//using namespace normal::pushdown::cache;
//using namespace normal::pushdown::merge;
//using namespace normal::pushdown::bloomjoin;
//using namespace normal::pushdown::file;
//
//namespace normal::ssb::common {
//
//class Operators {
//
//public:
//
//  static std::vector<std::shared_ptr<CacheLoad>>
//  makeFileCacheLoadOperators(const std::string &namePrefix,
//							 const std::string &filename,
//							 const std::vector<std::string> &columns,
//							 const std::string &dataDir,
//							 int numConcurrentUnits,
//							 const std::shared_ptr<OperatorGraph> &g);
//
//  static std::vector<std::shared_ptr<CacheLoad>>
//  makeCacheLoadOperators(const std::string &namePrefix,
//									const std::shared_ptr<Partition>& partition,
//									const std::vector<std::string> &columns,
//									int numConcurrentUnits,
//									const std::shared_ptr<OperatorGraph> &g);
//
////  static std::vector<std::shared_ptr<FileScan>>
////  makeFileScanOperators(const std::string &namePrefix,
////						const std::string &filename,
////						const std::vector<std::string> &columns,
////						const std::string &dataDir,
////						int numConcurrentUnits,
////						const std::shared_ptr<OperatorGraph> &g);
//
//  static std::vector<std::shared_ptr<FileScan>>
//  makeFileScanOperators(const std::string &namePrefix,
//								   const std::string &filename,
//								   FileType fileType,
//								   const std::vector<std::string> &columns,
//								   const std::string &dataDir,
//								   int numConcurrentUnits,
//								   const std::shared_ptr<OperatorGraph> &g);
//
//  static std::vector<std::shared_ptr<S3SelectScan2>>
//  makeS3SelectScanPushDownOperators(const std::string &namePrefix,
//									const std::string &s3Object,
//									const std::string &s3Bucket,
//									FileType fileType,
//									const std::vector<std::string> &columns,
//									const std::string &sql,
//									bool scanOnStart,
//									int numConcurrentUnits,
//									const std::shared_ptr<S3SelectPartition>& partition,
//									const std::shared_ptr<arrow::Schema>& schema,
//									AWSClient &client,
//									const std::shared_ptr<OperatorGraph> &g);
//
//  static std::vector<std::shared_ptr<Merge>>
//  makeMergeOperators(const std::string &namePrefix,
//					 int numConcurrentUnits,
//					 const std::shared_ptr<OperatorGraph> &g);
//
//  static std::shared_ptr<BloomCreateOperator>
//  makeBloomCreateOperator(const std::string &namePrefix,
//							   const std::string& columnName,
//							   double desiredFalsePositiveRate,
//							   const std::shared_ptr<OperatorGraph>& g);
//
//  static std::vector<std::shared_ptr<FileScanBloomUseOperator>>
//  makeFileScanBloomUseOperators(const std::string &namePrefix,
//								const std::string &filename,
//								const std::vector<std::string> &columns,
//								const std::string& columnName,
//								const std::string &dataDir,
//								int numConcurrentUnits,
//								const std::shared_ptr<OperatorGraph> &g);
//
//  static std::vector<std::shared_ptr<Shuffle>>
//  makeShuffleOperators(const std::string &namePrefix,
//					   const std::string &columnName,
//					   int numConcurrentUnits,
//					   const std::shared_ptr<OperatorGraph> &g);
//
//  static std::vector<std::shared_ptr<HashJoinBuild>>
//  makeHashJoinBuildOperators(const std::string &namePrefix,
//							 const std::string &columnName,
//							 int numConcurrentUnits,
//							 const std::shared_ptr<OperatorGraph> &g);
//
//  static std::vector<std::shared_ptr<HashJoinProbe>>
//  makeHashJoinProbeOperators(const std::string &namePrefix,
//							 const std::string &leftColumnName,
//							 const std::string &rightColumnName,
//							 int numConcurrentUnits,
//							 const std::shared_ptr<OperatorGraph> &g);
//
//  static std::shared_ptr<Collate> makeCollateOperator(const std::shared_ptr<OperatorGraph> &g);
//
//  template <typename A, typename B>
//  static void connectToEach(std::vector<std::shared_ptr<A>> producers,
//							std::vector<std::shared_ptr<B>> consumers){
//	assert(producers.size() == consumers.size());
//
//	for (size_t u = 0; u < producers.size(); ++u) {
//	  producers[u]->produce(consumers[u]);
//	  consumers[u]->consume(producers[u]);
//	}
//  }
//
//  template <typename A, typename B>
//  static void connectToAll(std::vector<std::shared_ptr<A>> producers,
//						   std::vector<std::shared_ptr<B>> consumers){
//
//	for (size_t u1 = 0; u1 < producers.size(); ++u1) {
//	  for (size_t u2 = 0; u2 < consumers.size(); ++u2) {
//		producers[u1]->produce(consumers[u2]);
//		consumers[u2]->consume(producers[u1]);
//	  }
//	}
//  }
//
//  template <typename A, typename B>
//  static void connectToAll(std::shared_ptr<A> producer,
//						   std::vector<std::shared_ptr<B>> consumers){
//
//	for (size_t u2 = 0; u2 < consumers.size(); ++u2) {
//	  producer->produce(consumers[u2]);
//	  consumers[u2]->consume(producer);
//	}
//  }
//
//  template <typename A, typename B>
//  static void connectToOne(std::vector<std::shared_ptr<A>> producers,
//						   std::shared_ptr<B> consumer){
//
//	for (size_t u1 = 0; u1 < producers.size(); ++u1) {
//	  producers[u1]->produce(consumer);
//	  consumer->consume(producers[u1]);
//	}
//  }
//
//  template <typename A, typename B>
//  static void connectToOne(std::shared_ptr<A> producer,
//						   std::shared_ptr<B> consumer){
//
//	producer->produce(consumer);
//	consumer->consume(producer);
//  }
//
//
//  template <typename B>
//  static void connectHitsToEach(std::vector<std::shared_ptr<CacheLoad>> producers,
//								std::vector<std::shared_ptr<B>> consumers){
//
//	for (size_t u1 = 0; u1 < producers.size(); ++u1) {
//	  producers[u1]->setHitOperator(consumers[u1]);
//	  consumers[u1]->consume(producers[u1]);
//	}
//  }
//
//  static void connectHitsToEachLeft(std::vector<std::shared_ptr<CacheLoad>> producers,
//								std::vector<std::shared_ptr<Merge>> consumers){
//
//	for (size_t u1 = 0; u1 < producers.size(); ++u1) {
//	  producers[u1]->setHitOperator(consumers[u1]);
//	  consumers[u1]->setLeftProducer(producers[u1]);
//	}
//  }
//
//  template <typename A>
//  static void connectToEachRight(std::vector<std::shared_ptr<A>> producers,
//									std::vector<std::shared_ptr<Merge>> consumers){
//
//	for (size_t u1 = 0; u1 < producers.size(); ++u1) {
//	  producers[u1]->produce(consumers[u1]);
//	  consumers[u1]->setRightProducer(producers[u1]);
//	}
//  }
//
//
//  template <typename B>
//  static void connectMissesToEach(std::vector<std::shared_ptr<CacheLoad>> producers,
//								  std::vector<std::shared_ptr<B>> consumers){
//
//	for (size_t u1 = 0; u1 < producers.size(); ++u1) {
//	  producers[u1]->setMissOperatorToCache(consumers[u1]);
////	  producers[u1]->setMissOperatorToPushdown(consumers[u1]); ????????????
//	  consumers[u1]->consume(producers[u1]);
//	}
//  }
//
//};
//
//}
//

#endif //NORMAL_NORMAL_SSB_INCLUDE_NORMAL_SSB_COMMON_OPERATORS_H
