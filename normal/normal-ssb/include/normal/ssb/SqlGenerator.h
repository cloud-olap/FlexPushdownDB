//
// Created by Yifei Yang on 8/8/20.
//

#ifndef NORMAL_NORMAL_SSB_INCLUDE_NORMAL_SSB_SQLGENERATOR_H
#define NORMAL_NORMAL_SSB_INCLUDE_NORMAL_SSB_SQLGENERATOR_H

#include <normal/ssb/Globals.h>
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

class SqlGenerator {

public:
  SqlGenerator();
  std::vector<std::string> generateSqlBatch (int batchSize);
  std::string generateSql (std::string queryName);

private:
  std::default_random_engine generator_;
  std::map<std::string, QueryName> queryNameMap_;

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
  int genLo_discount();
  int genLo_quantity();
  int genP_category_num();
  int genP_brand1_num();
  int genP_mfgr_num();
  std::string genS_region();
  std::string genS_nation();
  std::string genS_city();
};

}


#endif //NORMAL_NORMAL_SSB_INCLUDE_NORMAL_SSB_SQLGENERATOR_H
