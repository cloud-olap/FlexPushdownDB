//
// Created by Yifei Yang on 7/9/20.
//
//
//#include "ExperimentUtil.h"
//#include <aws/s3/model/ListObjectsRequest.h>
//#include <normal/connector/s3/S3SelectConnector.h>
//#include <normal/connector/s3/S3Util.h>
//#include <normal/connector/s3/S3SelectPartition.h>
//#include <normal/connector/s3/S3SelectExplicitPartitioningScheme.h>
//#include <normal/connector/s3/S3SelectCatalogueEntry.h>
//#include <normal/pushdown/Globals.h>
//#include <normal/pushdown/s3/S3Get.h>
//
//using namespace normal::plan::operator_::mode;
//using namespace normal::cache;
//using namespace normal::pushdown;
//using namespace normal::pushdown::s3;
//using namespace normal::sql;
//using namespace normal::connector;
//using namespace normal::connector::s3;
//
//namespace normal::ssb{
//
//std::string read_file(const std::string& filename) {
//  std::ifstream ifile(filename);
//  std::ostringstream buf;
//  char ch;
//  while (buf && ifile.get(ch)) {
//    buf.put(ch);
//  }
//  return buf.str();
//}
//
//[[maybe_unused]] std::vector<std::string> list_objects(AWSClient client, const std::string& bucket_name, const std::string& dir_prefix) {
//  client.init();
//  Aws::S3::S3Client s3Client = *AWSClient::defaultS3Client();
//  Aws::String aws_bucket_name(bucket_name);
//  Aws::String aws_dir_prefix(dir_prefix);
//
//  Aws::S3::Model::ListObjectsRequest objects_request;
//  objects_request.WithBucket(aws_bucket_name);
//  objects_request.WithPrefix(aws_dir_prefix);
//  auto list_objects_outcome = s3Client.ListObjects(objects_request);
//
//  std::vector<std::string> object_keys;
//
//  if (list_objects_outcome.IsSuccess())
//  {
//    Aws::Vector<Aws::S3::Model::Object> object_list =
//            list_objects_outcome.GetResult().GetContents();
//
//    for (auto const &s3_object : object_list)
//    {
//      object_keys.emplace_back(s3_object.GetKey().c_str());
//    }
//
//  }
//  else
//  {
//    std::cout << "ListObjects error: " <<
//              list_objects_outcome.GetError().GetExceptionName() << " " <<
//              list_objects_outcome.GetError().GetMessage() << std::endl;
//
//  }
//
//  return object_keys;
//}
//
//void configureS3ConnectorSinglePartition(Interpreter &i, const std::string& bucket_name, const std::string& dir_prefix) {
//  auto conn = std::make_shared<S3SelectConnector>("s3");
//  auto cat = std::make_shared<Catalogue>("s3", conn);
//
//  // look up tables
//  auto tableNames = defaultMiniCatalogue->tables();
//  auto s3Objects = std::make_shared<std::vector<std::string>>();
//  for (const auto &tableName: *tableNames) {
//    auto s3Object = dir_prefix + tableName + ".tbl";
//    s3Objects->emplace_back(s3Object);
//  }
//  auto objectNumBytes_Map = S3Util::listObjects(bucket_name, dir_prefix, *s3Objects, DefaultS3Client);
//
//  // configure s3Connector
//  for (size_t tbl_id = 0; tbl_id < tableNames->size(); tbl_id++) {
//    auto &tableName = tableNames->at(tbl_id);
//    auto &s3Object = s3Objects->at(tbl_id);
//    auto numBytes = objectNumBytes_Map.find(s3Object)->second;
//    auto partitioningScheme = std::make_shared<S3SelectExplicitPartitioningScheme>();
//    partitioningScheme->add(std::make_shared<S3SelectPartition>(bucket_name, s3Object, numBytes));
//    cat->put(std::make_shared<S3SelectCatalogueEntry>(tableName, partitioningScheme, cat));
//  }
//  i.put(cat);
//}
//
//void configureS3ConnectorMultiPartition(Interpreter &i, const std::string& bucket_name, const std::string& dir_prefix) {
//  auto conn = std::make_shared<S3SelectConnector>("s3");
//  auto cat = std::make_shared<Catalogue>("s3", conn);
//
//  // get partitionNums
//  auto s3ObjectsMap = std::make_shared<std::unordered_map<std::string, std::shared_ptr<std::vector<std::string>>>>();
//  auto partitionNums = defaultMiniCatalogue->partitionNums();
//  std::string fileExtension = getFileExtensionByDirPrefix(dir_prefix);
//  for (auto const &partitionNumEntry: *partitionNums) {
//    auto tableName = partitionNumEntry.first;
//    auto partitionNum = partitionNumEntry.second;
//    auto objects = std::make_shared<std::vector<std::string>>();
//    if (partitionNum == 1) {
//      objects->emplace_back(dir_prefix + tableName + fileExtension);
//      s3ObjectsMap->emplace(tableName, objects);
//    } else {
//      for (int j = 0; j < partitionNum; j++) {
//        objects->emplace_back(fmt::format("{0}{1}_sharded/{1}{2}.{3}", dir_prefix, tableName, fileExtension, j));
//      }
//      s3ObjectsMap->emplace(tableName, objects);
//    }
//  }
//
//  // look up tables
//  auto s3Objects = std::make_shared<std::vector<std::string>>();
//  for (auto const &s3ObjectPair: *s3ObjectsMap) {
//    auto objects = s3ObjectPair.second;
//    s3Objects->insert(s3Objects->end(), objects->begin(), objects->end());
//  }
//  auto objectNumBytes_Map = S3Util::listObjects(bucket_name, dir_prefix, *s3Objects, DefaultS3Client);
//
//  // configure s3Connector
//  for (auto const &s3ObjectPair: *s3ObjectsMap) {
//    auto tableName = s3ObjectPair.first;
//    auto objects = s3ObjectPair.second;
//    auto partitioningScheme = std::make_shared<S3SelectExplicitPartitioningScheme>();
//    for (auto const &s3Object: *objects) {
//      auto numBytes = objectNumBytes_Map.find(s3Object)->second;
//      partitioningScheme->add(std::make_shared<S3SelectPartition>(bucket_name, s3Object, numBytes));
//    }
//    cat->put(std::make_shared<S3SelectCatalogueEntry>(tableName, partitioningScheme, cat));
//  }
//  i.put(cat);
//}
//
//std::shared_ptr<TupleSet> execute(Interpreter &i) {
//  i.getCachingPolicy()->onNewQuery();
//  i.getOperatorGraph()->boot();
//  i.getOperatorGraph()->start();
//  i.getOperatorGraph()->join();
//
//  auto tuples = std::static_pointer_cast<Collate>(i.getOperatorGraph()->getOperator("collate"))->tuples();
//
//  return tuples;
//}
//
//std::shared_ptr<TupleSet2> executeSql(Interpreter &i, const std::string &sql, bool saveMetrics, bool writeResults, const std::string& outputFileName) {
//  i.clearOperatorGraph();
//  i.parse(sql);
//
//  auto tuples = execute(i);
//
//  // FIXME: if result is no tuples?
//  std::shared_ptr<TupleSet2> tupleSet;
//  if (tuples)
//    tupleSet = TupleSet2::create(tuples);
//  else
//    tupleSet = TupleSet2::make();
//
//  // Once the query is done there should be no active get conversion in S3Get.cpp, so this value should be 0 unless
//  // there are background caching operations still performing GET/Select.
//  SPDLOG_INFO("Done with query, activeGetConversions = {}", activeGetConversions);
//  if (writeResults) {
//    auto outputdir = std::filesystem::current_path().append("outputs");
//    std::filesystem::create_directory(outputdir);
//    auto outputFile = outputdir.append(outputFileName);
//    std::ofstream fout(outputFile.string());
//    fout << "Output  |\n" << tupleSet->showString(TupleSetShowOptions(TupleSetShowOrientation::RowOriented, tupleSet->numRows()));
//    fout.flush();
//    fout.close();
//  }
////  SPDLOG_INFO("Output  |\n{}", tupleSet->showString(TupleSetShowOptions(TupleSetShowOrientation::RowOriented)));
////  SPDLOG_INFO("Output size: {}", tupleSet->size());
//  SPDLOG_CRITICAL("Output rows: {}", tupleSet->numRows());
////  if (saveMetrics)
//  SPDLOG_INFO("Metrics:\n{}", i.getOperatorGraph()->showMetrics());
//  SPDLOG_CRITICAL("Finished, time: {} secs", (double) (i.getOperatorGraph()->getElapsedTime().value()) / 1000000000.0);
////  SPDLOG_INFO("Current cache layout:\n{}", i.getCachingPolicy()->showCurrentLayout());
////  SPDLOG_INFO("Memory allocated: {}", arrow::default_memory_pool()->bytes_allocated());
//  if (saveMetrics) {
//    i.saveMetrics();
//    i.saveHitRatios();
//  }
//
//  i.getOperatorGraph().reset();
////  std::this_thread::sleep_for (std::chrono::milliseconds (2000));
//  return tupleSet;
//}
//
//void generateBeladySegmentKeyAndSqlQueryMappings(const std::shared_ptr<Mode>& mode, const std::shared_ptr<BeladyCachingPolicy>& beladyCachingPolicy,
//                                                 const std::string& bucket_name, const std::string& dir_prefix, int numQueries, std::filesystem::path sql_file_dir_path) {
//  Interpreter i(mode, beladyCachingPolicy);
//  configureS3ConnectorMultiPartition(i, bucket_name, dir_prefix);
//  i.boot();
//  // populate mapping of SegmentKey->[Query #s Segment is used in] and
//  // QueryNumber->[Involved Segment Keys] and set these in the beladyMiniCatalogue
//  for (auto queryNum = 1; queryNum <= numQueries; ++queryNum) {
//    SPDLOG_INFO("processing segments in query {}", queryNum);
//    auto sql_file_path = sql_file_dir_path.append(fmt::format("{}.sql", queryNum));
//    auto sql = read_file(sql_file_path.string());
//    // get the related segments from the query:
//    i.clearOperatorGraph();
//    i.parse(sql);
//
//    auto logicalPlan = i.getLogicalPlan();
//    auto logicalOperators = logicalPlan->getOperators();
//    for (const auto& logicalOp: *logicalOperators) {
//      auto involvedSegmentKeys = logicalOp->extractSegmentKeys();
//      for (const auto& segmentKey: *involvedSegmentKeys) {
//        beladyMiniCatalogue->addToSegmentQueryNumMappings(queryNum, segmentKey);
//      }
//    }
//    i.getOperatorGraph().reset();
//    sql_file_dir_path = sql_file_dir_path.parent_path();
//  }
//}
//
//}