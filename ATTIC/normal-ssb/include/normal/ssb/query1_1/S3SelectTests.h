//
// Created by matt on 6/7/20.
//

#ifndef NORMAL_NORMAL_SSB_INCLUDE_NORMAL_SSB_QUERY1_1_S3SELECTTESTS_H
#define NORMAL_NORMAL_SSB_INCLUDE_NORMAL_SSB_QUERY1_1_S3SELECTTESTS_H

//#include <string>
//#include <normal/core/ATTIC/Normal.h>
//#include <normal/tuple/FileType.h>
//
//using namespace normal::core;
//using namespace normal::tuple;
//
//namespace normal::ssb {
//
//class S3SelectTests {
//
//public:
//
//  /**
//   * Tests that SQLLite and Normal produce the same output for date scan component of query 1.1
//   *
//   * Only checking row count at moment
//   */
//  static void dateScan(const std::string &s3ObjectDir,
//					   const std::string &dataDir,
//					   FileType fileType,
//					   int numConcurrentUnits,
//					   int numIterations,
//					   bool check,
//					   const std::shared_ptr<Normal> &n);
//
//  /**
//   * Tests that SQLLite and Normal produce the same output for date scan component of query 1.1 using hybrid strategy.
//   *
//   * Only checking row count at moment
//   */
//  static void hybridDateFilter(short year,
//							   const std::string &s3ObjectDir,
//							   const std::string &dataDir,
//							   FileType fileType,
//							   int numConcurrentUnits,
//							   int numIterations,
//							   bool check,
//							   const std::shared_ptr<Normal> &n);
//
//  static void dateFilter(short year,
//						 const std::string &s3ObjectDir,
//						 const std::string &dataDir,
//						 FileType fileType,
//						 int numConcurrentUnits,
//						 bool check,
//						 const std::shared_ptr<Normal> &n);
//
//  static void lineOrderScan(const std::string &s3ObjectDir,
//							const std::string &dataDir,
//							FileType fileType,
//							int numConcurrentUnits,
//							bool check,
//							const std::shared_ptr<Normal> &n);
//
//  static void lineOrderFilter(short discount,
//							  short quantity,
//							  const std::string &s3ObjectDir,
//							  const std::string &dataDir,
//							  FileType fileType,
//							  int numConcurrentUnits,
//							  bool check,
//							  const std::shared_ptr<Normal> &n);
//
//  static void join(short year,
//				   short discount,
//				   short quantity,
//				   const std::string &s3ObjectDir,
//				   const std::string &dataDir,
//				   FileType fileType,
//				   int numConcurrentUnits,
//				   bool check,
//				   const std::shared_ptr<Normal> &n);
//
//  static void full(short year, short discount, short quantity,
//				   const std::string &s3ObjectDir, const std::string &dataDir,
//				   FileType fileType,
//				   int numConcurrentUnits,
//				   bool check,
//				   const std::shared_ptr<Normal> &n);
//
//  static void fullPushDown(short year, short discount, short quantity,
//						   const std::string &s3ObjectDir, const std::string &dataDir,
//						   FileType fileType,
//						   int numConcurrentUnits,
//						   bool check,
//						   const std::shared_ptr<Normal> &n);
//
//  static void hybrid(short year, short discount, short quantity,
//					 const std::string &s3ObjectDir, const std::string &dataDir,
//					 FileType fileType,
//					 int numConcurrentUnits,
//					 bool check,
//					 const std::shared_ptr<Normal> &n);
//
//};
//
//}

#endif //NORMAL_NORMAL_SSB_INCLUDE_NORMAL_SSB_QUERY1_1_S3SELECTTESTS_H
