//
// Created by Yifei Yang on 8/8/20.
//

#ifndef NORMAL_NORMAL_SSB_INCLUDE_NORMAL_SSB_SQLGENERATOR_H
#define NORMAL_NORMAL_SSB_INCLUDE_NORMAL_SSB_SQLGENERATOR_H

#include <memory>
#include <map>
#include <random>

namespace normal::ssb {

enum QueryName {
  Query1_1, Query1_2, Query1_3,
  Query2_1, Query2_2, Query2_3,
  Query3_1, Query3_2, Query3_3, Query3_4,
  Query4_1, Query4_2, Query4_3,
  Unknown
};

enum SkewQueryName {
  SkewQuery2_1, SkewQuery2_2, SkewQuery2_3,
  SkewQuery3_1, SkewQuery3_2, SkewQuery3_3, SkewQuery3_4,
  SkewQuery4_1, SkewQuery4_2, SkewQuery4_3,
  SkewUnknown
};

enum SkewWeightQueryName {
  SkewWeightQuery1,
  SkewWeightQuery2,
  SkewWeightQuery3,
  SkewWeightQuery4,
  SkewWeightUnknown
};

class SqlGenerator {

public:
  SqlGenerator();
  void setLineorderRowSelectivity(double lineorderRowSelectivity);

  std::vector<std::string> generateSqlBatch(int batchSize);
  std::string generateSql(const std::string& queryName);

  /**
   * The following is used to generate skew benchmark
   */
  std::vector<std::string> generateSqlBatchSkew(float skewness, int batchSize);
  std::string generateSqlSkew(std::string queryName, std::string skewLo_predicate);

  /**
   * The following is used to generate skew benchmark + skew column selectivity (weight)
   * Currently a simple one: 2 high-selectivity columns and 2 low-selectivity columns in lineorder
   */
  std::vector<std::string> generateSqlBatchSkewWeight(float skewness, int batchSize);
  std::string generateSqlSkewWeight(std::string queryName, std::string skewLo_predicate, bool high);

  /**
   * The following is used to generate recurring queries with skewness, recurring order is same as SSB
   * No weight skewness involved
   */
  std::vector<std::string> generateSqlBatchSkewRecurring(float skewness);

  /**
   * The following is used to generate the hot-query workload
   */
  std::vector<std::string> generateSqlBatchHotQuery(float percentage, int batchSize);

  /**
   * The following is used to generated two consecutive queries for model evaluation,
   * where the first is for caching. Ignore join, aggregation and group by.
   * @param: hit ratio is expected, may be slightly different from the real
   * @param: rowPer and nCol together contribute to k
   */
  std::vector<std::string> generateSqlForMathModel(double hitRatio, double rowPer, int nCol);

private:
  std::shared_ptr<std::default_random_engine> generator_;
  std::map<std::string, QueryName> queryNameMap_;
  std::map<std::string, SkewQueryName> skewQueryNameMap_;
  std::map<std::string, SkewWeightQueryName> skewWeightQueryNameMap_;
  double lineorderRowSelectivity_ = 0.2;    // default from SSB

  std::string genQuery1_1();
  std::string genQuery1_2();
  std::string genQuery1_3();
  std::string genQuery2_1();
  std::string genQuery2_2();
  std::string genQuery2_3();
  std::string genQuery3_1();
  std::string genQuery3_2();
  std::string genQuery3_3();
  std::string genQuery3_4();
  std::string genQuery4_1();
  std::string genQuery4_2();
  std::string genQuery4_3();

  int genD_year();
  int genD_yearmonthnum();
  int genD_weeknuminyear();
  std::string genD_yearmonth();
  std::string genD_dayofweek();
  int genLo_discount(int min, int max);
  int genLo_quantity(int min, int max);
  std::string genLo_predicate();
  int genP_category_num();
  int genP_brand1_num();
  int genP_mfgr_num();
  std::string genS_region();
  std::string genS_nation();
  std::string genS_city();

  /**
   * The following is used to generate skew benchmark
   */
  std::string genSkewQuery2_1(std::string skewLo_predicate, std::string lo_predicate, std::string aggColumn);
  std::string genSkewQuery2_2(std::string skewLo_predicate, std::string lo_predicate, std::string aggColumn);
  std::string genSkewQuery2_3(std::string skewLo_predicate, std::string lo_predicate, std::string aggColumn);
  std::string genSkewQuery3_1(std::string skewLo_predicate, std::string lo_predicate, std::string aggColumn);
  std::string genSkewQuery3_2(std::string skewLo_predicate, std::string lo_predicate, std::string aggColumn);
  std::string genSkewQuery3_3(std::string skewLo_predicate, std::string lo_predicate, std::string aggColumn);
  std::string genSkewQuery3_4(std::string skewLo_predicate, std::string lo_predicate, std::string aggColumn);
  std::string genSkewQuery4_1(std::string skewLo_predicate, std::string lo_predicate, std::string aggColumn);
  std::string genSkewQuery4_2(std::string skewLo_predicate, std::string lo_predicate, std::string aggColumn);
  std::string genSkewQuery4_3(std::string skewLo_predicate, std::string lo_predicate, std::string aggColumn);

  /**
   * The following is used to generate skew benchmark with column selectivity
   */
  std::string genLo_predicate(bool high);
  std::string genSkewWeightQuery1(std::string skewLo_predicate, std::string lo_predicate, std::string aggColumn);
  std::string genSkewWeightQuery2(std::string skewLo_predicate, std::string lo_predicate, std::string aggColumn);
  std::string genSkewWeightQuery3(std::string skewLo_predicate, std::string lo_predicate, std::string aggColumn);
  std::string genSkewWeightQuery4(std::string skewLo_predicate, std::string lo_predicate, std::string aggColumn);
};

}


#endif //NORMAL_NORMAL_SSB_INCLUDE_NORMAL_SSB_SQLGENERATOR_H
