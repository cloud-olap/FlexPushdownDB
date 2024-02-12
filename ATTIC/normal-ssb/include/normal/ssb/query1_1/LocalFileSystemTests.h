//
// Created by matt on 6/7/20.
//

#ifndef NORMAL_NORMAL_SSB_INCLUDE_NORMAL_SSB_QUERY1_1_LOCALFILESYSTEMTESTS_H
#define NORMAL_NORMAL_SSB_INCLUDE_NORMAL_SSB_QUERY1_1_LOCALFILESYSTEMTESTS_H
//
//#include <string>
//
//#include <normal/core/ATTIC/Normal.h>
//#include <normal/tuple/FileType.h>
//
//using namespace normal::core;
//using namespace normal::tuple;
//
//namespace normal::ssb {
//
///**
// * Test executors for partial and full queries using Normal and SQLLite against local file system data sources.
// * Compares the results of each.
// *
// * TODO: This can be refactored once SQLLite produces tuple sets that can automatically compared.
// */
//class LocalFileSystemTests {
//
//public:
//
//  static void dateScan(const std::string &dataDir,
//					   FileType fileType,
//					   int numConcurrentUnits,
//					   int numIterations,
//					   bool check,
//					   const std::shared_ptr<Normal> &n);
//
//  static void dateFilter(short year,
//						 const std::string &dataDir,
//						 FileType fileType,
//						 int numConcurrentUnits,
//						 bool check,
//						 const std::shared_ptr<Normal> &n);
//
//  static void lineOrderScan(const std::string &dataDir,
//							FileType fileType,
//							int numConcurrentUnits,
//							int numIterations,
//							bool check,
//							const std::shared_ptr<Normal> &n);
//
//  static void lineOrderFilter(short discount,
//							  short quantity,
//							  const std::string &dataDir,
//							  FileType fileType,
//							  int numConcurrentUnits,
//							  bool check,
//							  const std::shared_ptr<Normal> &n);
//
//  static void join(short year,
//				   short discount,
//				   short quantity,
//				   const std::string &dataDir,
//				   FileType fileType,
//				   int numConcurrentUnits,
//				   bool check,
//				   const std::shared_ptr<Normal> &n);
//
//  static void full2(short year, short discount, short quantity,
//					const std::string &dataDir,
//					FileType fileType,
//					int numConcurrentUnits,
//					int numIterations,
//					bool check,
//					const std::shared_ptr<Normal> &n);
//
//  static void bloom(short year,
//					short discount,
//					short quantity,
//					const std::string &dataDir,
//					FileType fileType,
//					int numConcurrentUnits,
//					bool check,
//					const std::shared_ptr<Normal> &n);
//};
//}

#endif //NORMAL_NORMAL_SSB_INCLUDE_NORMAL_SSB_QUERY1_1_LOCALFILESYSTEMTESTS_H
