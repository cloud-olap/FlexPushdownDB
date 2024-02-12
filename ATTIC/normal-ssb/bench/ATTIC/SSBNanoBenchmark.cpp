//
// Created by Jialing Pei on 6/22/20.
//
//
// Created by matt on 24/4/20.
//
//
//#include <vector>
//
//#include <doctest/doctest.h>
//#include <nanobench.h>
//
//#include <normal/tuple/arrow/Arrays.h>
//#include <normal/tuple/TupleSet.h>
//#include <normal/core/type/DecimalType.h>
//#include <normal/core/type/Float64Type.h>
//
//
//
//#include <normal/expression/gandiva/Expression.h>
//#include <normal/expression/gandiva/Column.h>
//#include <normal/expression/gandiva/Cast.h>
//#include <normal/expression/gandiva/Projector.h>
//#include <normal/connector/local-fs/LocalFilePartition.h>
//
//#include <normal/cache/SegmentCache.h>
//#include <normal/cache/SegmentKey.h>
//#include <normal/cache/SegmentData.h>
//#include <normal/cache/SegmentRange.h>
//#include <normal/cache/LRUCachingPolicy.h>
//#include <normal/core/cache/LoadResponseMessage.h>
//
//#include <experimental/filesystem>
//
//#include <normal/ssb/Globals.h>
//#include <normal/core/OperatorManager.h>
//#include <normal/pushdown/collate/Collate.h>
//#include <normal/pushdown/file/FileScan.h>
//#include <normal/pushdown/aggregate/AggregationFunction.h>
//#include <normal/pushdown/aggregate/Sum.h>
//#include <normal/pushdown/aggregate/Aggregate.h>
//#include <normal/expression/gandiva/Column.h>
//#include <normal/core/type/Float64Type.h>
//#include <normal/core/type/Integer32Type.h>
//#include <normal/core/type/Integer64Type.h>
//#include <normal/expression/gandiva/Cast.h>
//#include <normal/expression/gandiva/Multiply.h>
//#include <normal/pushdown/join/HashJoinBuild.h>
//#include <normal/pushdown/join/HashJoinProbe.h>
//#include <normal/pushdown/filter/Filter.h>
//#include <normal/pushdown/filter/FilterPredicate.h>
//#include <normal/expression/gandiva/NumericLiteral.h>
//#include <normal/expression/gandiva/LessThan.h>
//#include <normal/expression/gandiva/EqualTo.h>
//#include <normal/expression/gandiva/LessThanOrEqualTo.h>
//#include <normal/expression/gandiva/GreaterThanOrEqualTo.h>
//#include <normal/expression/gandiva/And.h>
//#include <normal/pushdown/s3/S3SelectScan.h>
//#include <normal/pushdown/AWSClient.h>
//#include <aws/s3/model/ListObjectsV2Request.h>
//#include <aws/s3/model/ListObjectsV2Result.h>
//#include <normal/connector/s3/S3Util.h>
//#include <normal/pushdown/Util.h>
//
//
//#define SKIP_SUITE false
//
//using namespace normal::core::type;
//
//
////std::shared_ptr<normal::core::cache::LoadResponseMessage> prepareResponseMessage(){
////    auto cache = normal::cache::SegmentCache::make(normal::cache::LRUCachingPolicy::make(100));
////    auto segment1Partition1 = std::make_shared<LocalFilePartition>("data/a.csv");
////    auto segment1Key1 = normal::cache::SegmentKey::make(segment1Partition1, "a",normal::cache::SegmentRange::make(0, 1023));
////
////    auto segment1Column1 = Column::make("a", ::arrow::utf8());
////    auto segment1Data1 = normal::cache::SegmentData::make(segment1Column1);
////    std::unordered_map<std::shared_ptr<normal::cache::SegmentKey>, std::shared_ptr<normal::cache::SegmentData>> segments;
////    segments.insert(std::pair(segment1Key1, segment1Data1));
////    auto responseMessage = normal::core::cache::LoadResponseMessage::make(segments,"cache");
////    return responseMessage;
////
////}
////
////
////TEST_CASE ("nanobenchmark-q1.1-lineorderScan" * doctest::skip(false || SKIP_SUITE)) {
////    short year = 1992;
////    short discount = 2;
////    short quantity = 24;
////    std::string s3Bucket = "s3filter";
////    std::string s3ObjectDir = "ssb-sf0.01";
////    short numPartitions = 2;
////
////    SPDLOG_INFO("Arguments  |  s3Bucket: '{}', s3ObjectDir: '{}', numPartitions: {}, year: {}, discount: {}, quantity: {}",
////                s3Bucket, s3ObjectDir, numPartitions, year, discount, quantity);
////
////    normal::pushdown::AWSClient client;
////    client.init();
////    auto lineOrderFile = s3ObjectDir + "/lineorder.tbl";
////    auto dateFile = s3ObjectDir + "/date.tbl";
////    auto s3Objects = std::vector{lineOrderFile, dateFile};
////
////    auto partitionMap = normal::connector::s3::S3Util::listObjects(s3Bucket, s3Objects, client.defaultS3Client());
////
////    SPDLOG_DEBUG("Discovered partitions");
////    for (auto &partition : partitionMap) {
////        SPDLOG_DEBUG("  's3://{}/{}': size: {}", s3Bucket, partition.first, partition.second);
////    }
////
////    /**
////   * Scan
////   * lineorder.tbl
////   */
////    std::vector<std::string> lineOrderColumns =
////            {"LO_ORDERDATE", "LO_QUANTITY", "LO_EXTENDEDPRICE", "LO_DISCOUNT", "LO_REVENUE"};
////    int discountLower = discount - 1;
////    int discountUpper = discount + 1;
////
////    std::vector<std::shared_ptr<Operator>> lineOrderScanOperators;
////    auto lineOrderScanRanges = normal::pushdown::Util::ranges<long>(0, partitionMap.find(lineOrderFile)->second, numPartitions);
////    auto msg = prepareResponseMessage();
////    normal::core::message::Envelope e(msg);
////    for (int p = 0; p < numPartitions; ++p) {
////        auto lineOrderScan = normal::pushdown::S3SelectScan::make(
////                fmt::format("lineOrderScan-{}", p),
////                s3Bucket,
////                lineOrderFile,
////                fmt::format(
////                        "select LO_ORDERDATE, LO_QUANTITY, LO_EXTENDEDPRICE, LO_DISCOUNT, LO_REVENUE from s3Object where cast(LO_DISCOUNT as int) between {} and {} and cast(LO_QUANTITY as int) < {}",
////                        discountLower,
////                        discountUpper,
////                        quantity),
////                lineOrderColumns,
////                lineOrderScanRanges[p].first,
////                lineOrderScanRanges[p].second,
////                normal::pushdown::S3SelectCSVParseOptions(",", "\n"),
////                client.defaultS3Client());
////        lineOrderScanOperators.push_back(lineOrderScan);
////
////
////    }
////    auto mgr = std::make_shared<OperatorManager>();
////    for (int p = 0; p < numPartitions; ++p)
////        mgr->put(lineOrderScanOperators[p]);
////    mgr->boot();
////
////    for (int p = 0; p < numPartitions; ++p) {
////        ankerl::nanobench::Config().minEpochIterations(10).run(
////                "evaluate-linrorderscan", [&] {
////                    lineOrderScanOperators[p]->onReceive(e);
////                });
////    }
////
////
////
////
////}
//
//
