//
// Created by matt on 6/7/20.
//

#ifndef NORMAL_NORMAL_SSB_INCLUDE_NORMAL_SSB_QUERY1_1_LOCALFILESYSTEMTESTS_H
#define NORMAL_NORMAL_SSB_INCLUDE_NORMAL_SSB_QUERY1_1_LOCALFILESYSTEMTESTS_H

#include <string>

namespace normal::ssb {

/**
 * Test executors for partial and full queries using Normal and SQLLite. Compares the results of each.
 *
 * TODO: This can be refactored once SQLLite produces tuple sets that can automatically compared.
 */
class LocalFileSystemTests {

public:

  /**
   * LocalFileSystemTests that SQLLite and Normal produce the same output for date scan component of query 1.1
   *
   * Only checking row count at moment
   */
  static void dateScan(const std::string &dataDir, int numConcurrentUnits, bool check);

  /**
   * LocalFileSystemTests that SQLLite and Normal produce the same output for date scan and filter component of query 1.1
   *
   * Only checking row count at moment
   */
  static void dateFilter(short year, const std::string &dataDir, int numConcurrentUnits, bool check);

  /**
   * LocalFileSystemTests that SQLLite and Normal produce the same output for lineorder scan component of query 1.1
   *
   * Only checking row count at moment
   */
  static void lineOrderScan(const std::string &dataDir, int numConcurrentUnits, bool check);

  /**
   * LocalFileSystemTests that SQLLite and Normal produce the same output for lineorder scan and filter component of query 1.1
   *
   * Only checking row count at moment
   */
  static void lineOrderFilter(short discount, short quantity, const std::string &dataDir, int numConcurrentUnits, bool check);

  /**
   * LocalFileSystemTests that SQLLite and Normal produce the same output for join component of query 1.1
   *
   * Only checking row count at moment
   */
  static void join(short year, short discount, short quantity, const std::string &dataDir, int numConcurrentUnits, bool check);

  /**
   * LocalFileSystemTests that SQLLite and Normal produce the same output for full query 1.1
   */
  static void full(short year, short discount, short quantity,
				   const std::string &dataDir,
				   int numConcurrentUnits,
				   bool check);
};
}

#endif //NORMAL_NORMAL_SSB_INCLUDE_NORMAL_SSB_QUERY1_1_LOCALFILESYSTEMTESTS_H