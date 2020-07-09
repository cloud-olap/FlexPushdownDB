//
// Created by matt on 6/7/20.
//

#ifndef NORMAL_NORMAL_SSB_INCLUDE_NORMAL_SSB_QUERY1_1_S3SELECTTESTS_H
#define NORMAL_NORMAL_SSB_INCLUDE_NORMAL_SSB_QUERY1_1_S3SELECTTESTS_H

#include <string>

namespace normal::ssb {

class S3SelectTests {

public:

  /**
   * Tests that SQLLite and Normal produce the same output for date scan component of query 1.1
   *
   * Only checking row count at moment
   */
  static void dateScan(const std::string &s3ObjectDir,
					   const std::string &dataDir,
					   int numConcurrentUnits,
					   int numIterations,
					   bool check);

  /**
   * Tests that SQLLite and Normal produce the same output for date scan component of query 1.1 using hybrid strategy.
   *
   * Only checking row count at moment
   */
  static void hybridDateFilter(short year,
							   const std::string &s3ObjectDir,
							   const std::string &dataDir,
							   int numConcurrentUnits,
							   int numIterations,
							   bool check);

  static void dateFilter(short year,
						 const std::string &s3ObjectDir,
						 const std::string &dataDir,
						 int numConcurrentUnits,
						 bool check);

  static void lineOrderScan(const std::string &s3ObjectDir,
							const std::string &dataDir,
							int numConcurrentUnits,
							bool check);

  static void lineOrderFilter(short discount,
							  short quantity,
							  const std::string &s3ObjectDir,
							  const std::string &dataDir,
							  int numConcurrentUnits,
							  bool check);

  static void join(short year,
				   short discount,
				   short quantity,
				   const std::string &s3ObjectDir,
				   const std::string &dataDir,
				   int numConcurrentUnits,
				   bool check);

  static void full(short year, short discount, short quantity,
				   const std::string &s3ObjectDir, const std::string &dataDir,
				   int numConcurrentUnits,
				   bool check);

  static void fullPushDown(short year, short discount, short quantity,
						   const std::string &s3ObjectDir, const std::string &dataDir,
						   int numConcurrentUnits,
						   bool check);

  static void hybrid(short year, short discount, short quantity,
					 const std::string &s3ObjectDir, const std::string &dataDir,
					 int numConcurrentUnits,
					 bool check);

};

}

#endif //NORMAL_NORMAL_SSB_INCLUDE_NORMAL_SSB_QUERY1_1_S3SELECTTESTS_H
