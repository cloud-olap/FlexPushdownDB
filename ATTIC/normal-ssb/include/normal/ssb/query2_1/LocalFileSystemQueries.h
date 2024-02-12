//
// Created by matt on 10/8/20.
//

#ifndef NORMAL_NORMAL_SSB_INCLUDE_NORMAL_SSB_QUERY2_1_LOCALFILESYSTEMQUERIES_H
#define NORMAL_NORMAL_SSB_INCLUDE_NORMAL_SSB_QUERY2_1_LOCALFILESYSTEMQUERIES_H
//
//#include "normal/ssb/query2_1/LocalFileSystemQueries.h"
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
//namespace normal::ssb::query2_1 {
//
//class LocalFileSystemQueries {
//
//public:
//
//  static const inline std::vector<std::string> SupplierFields
//	  {"S_SUPPKEY", "S_REGION"};
//
//  static const inline std::vector<std::string> LineOrderFields
//	  { "LO_PARTKEY", "LO_SUPPKEY", "LO_ORDERDATE", "LO_REVENUE"};
//
//  static const inline std::vector<std::string> DateFields
//	  {"D_DATEKEY"};
//
//  static const inline std::vector<std::string> PartFields
//	  {"P_PARTKEY", "P_CATEGORY"};
//
//  static std::shared_ptr<OperatorGraph> partFilter(const std::string &dataDir,
//												   FileType fileType,
//											   const std::string &category,
//											   int numConcurrentUnits,
//											   const std::shared_ptr<Normal> &n);
//
//  static std::shared_ptr<OperatorGraph> join2x(const std::string &dataDir,
//											   FileType fileType,
//											   const std::string &region,
//											   int numConcurrentUnits,
//											   const std::shared_ptr<Normal> &n);
//
//  static std::shared_ptr<OperatorGraph> join3x(const std::string &dataDir,
//											   FileType fileType,
//											   const std::string &region,
//											   int numConcurrentUnits,
//											   const std::shared_ptr<Normal> &n);
//
//  static std::shared_ptr<OperatorGraph> join(const std::string &dataDir,
//											 FileType fileType,
//											 const std::string &category,
//											 const std::string &region,
//											 int numConcurrentUnits,
//											 const std::shared_ptr<Normal> &n);
//};
//
//}

#endif //NORMAL_NORMAL_SSB_INCLUDE_NORMAL_SSB_QUERY2_1_LOCALFILESYSTEMQUERIES_H
