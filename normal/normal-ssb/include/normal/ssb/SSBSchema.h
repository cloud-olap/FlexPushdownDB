//
// Created by matt on 10/8/20.
//

#ifndef NORMAL_NORMAL_SSB_INCLUDE_NORMAL_SSB_SSBSCHEMA_H
#define NORMAL_NORMAL_SSB_INCLUDE_NORMAL_SSB_SSBSCHEMA_H

#include <vector>
#include <string>
#include <normal/tuple/Schema.h>

class SSBSchema {
public:
  static const inline std::vector<std::string> SupplierFields
	  {"S_SUPPKEY", "S_NAME", "S_ADDRESS", "S_CITY", "S_NATION", "S_REGION", "S_PHONE"};

  static const inline std::vector<std::string> LineOrderFields
	  {"LO_ORDERKEY", "LO_LINENUMBER", "LO_CUSTKEY", "LO_PARTKEY", "LO_SUPPKEY", "LO_ORDERDATE", "LO_ORDERPRIORITY",
	   "LO_SHIPPRIORITY", "LO_QUANTITY", "LO_EXTENDEDPRICE", "LO_ORDTOTALPRICE", "LO_DISCOUNT", "LO_REVENUE",
	   "LO_SUPPLYCOST", "LO_TAX", "LO_COMMITDATE", "LO_SHIPMODE"};

  static const inline std::vector<std::string> DateFields
	  {"D_DATEKEY", "D_DATE", "D_DAYOFWEEK", "D_MONTH", "D_YEAR", "D_YEARMONTHNUM", "D_YEARMONTH", "D_DAYNUMINWEEK",
	   "D_DAYNUMINMONTH", "D_DAYNUMINYEAR", "D_MONTHNUMINYEAR", "D_WEEKNUMINYEAR", "D_SELLINGSEASON",
	   "D_LASTDAYINWEEKFL", "D_LASTDAYINMONTHFL", "D_HOLIDAYFL", "D_WEEKDAYFL"};

  static const inline std::vector<std::string> PartFields
	  {"P_PARTKEY", "P_NAME", "P_MFGR", "P_CATEGORY", "P_BRAND1", "P_COLOR", "P_TYPE", "P_SIZE", "P_CONTAINER"};

  static std::shared_ptr<arrow::Schema> customerSchema();
  static std::shared_ptr<arrow::Schema> dateSchema();
  static std::shared_ptr<arrow::Schema> lineOrder();
  static std::shared_ptr<arrow::Schema> part();
  static std::shared_ptr<arrow::Schema> supplier();
};

#endif //NORMAL_NORMAL_SSB_INCLUDE_NORMAL_SSB_SSBSCHEMA_H
