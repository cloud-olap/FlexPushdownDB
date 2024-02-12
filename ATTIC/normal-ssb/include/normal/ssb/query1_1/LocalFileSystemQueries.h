//
// Created by matt on 26/6/20.
//

#ifndef NORMAL_NORMAL_SSB_INCLUDE_NORMAL_SSB_QUERY1_1_LOCALFILESYSTEMQUERIES_H
#define NORMAL_NORMAL_SSB_INCLUDE_NORMAL_SSB_QUERY1_1_LOCALFILESYSTEMQUERIES_H
//
//#include <memory>
//
//#include <normal/core/OperatorManager.h>
//#include <normal/core/graph/OperatorGraph.h>
//#include <normal/core/ATTIC/Normal.h>
//#include <normal/tuple/FileType.h>
//
//using namespace normal::core;
//using namespace normal::core::graph;
//using namespace normal::tuple;
//
//namespace normal::ssb::query1_1 {
//
///**
// * Partial and full query definitions for SSB query 1.1 as Normal execution plans using local file system data sources
// */
//class LocalFileSystemQueries {
//
//public:
//
//  static std::shared_ptr<OperatorGraph> dateScan(const std::string &dataDir,
//												 FileType fileType,
//												 int numConcurrentUnits,
//												 const std::shared_ptr<Normal> &n);
//
//  static std::shared_ptr<OperatorGraph> lineOrderScan(const std::string &dataDir,
//													  FileType fileType,
//													  int numConcurrentUnits,
//													  const std::shared_ptr<Normal> &n);
//
//  static std::shared_ptr<OperatorGraph> dateFilter(const std::string &dataDir,
//												   FileType fileType,
//												   short year,
//												   int numConcurrentUnits,
//												   const std::shared_ptr<Normal> &n);
//
//  static std::shared_ptr<OperatorGraph> lineOrderFilter(const std::string &dataDir,
//														FileType fileType,
//														short discount,
//														short quantity,
//														int numConcurrentUnits,
//														const std::shared_ptr<Normal> &n);
//
//  static std::shared_ptr<OperatorGraph> join(const std::string &dataDir,
//											 FileType fileType,
//											 short year,
//											 short discount,
//											 short quantity,
//											 int numConcurrentUnits,
//											 const std::shared_ptr<Normal> &n);
//
//  static std::shared_ptr<OperatorGraph> full(const std::string &dataDir,
//											 FileType fileType,
//											 short year,
//											 short discount,
//											 short quantity,
//											 int numConcurrentUnits,
//											 const std::shared_ptr<Normal> &n);
//
//  static std::shared_ptr<OperatorGraph> bloom(const std::string &dataDir,
//											  FileType fileType,
//											  short year,
//											  short discount,
//											  short quantity,
//											  int numConcurrentUnits,
//											  const std::shared_ptr<Normal> &n);
//};
//
//}

#endif //NORMAL_NORMAL_SSB_INCLUDE_NORMAL_SSB_QUERY1_1_LOCALFILESYSTEMQUERIES_H
