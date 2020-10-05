//
// Created by matt on 10/8/20.
//

#include "normal/ssb/SSBSchema.h"

std::shared_ptr<arrow::Schema> SSBSchema::customer() {

  // 1,"Customer#000000001","j5JsirBM9P","MOROCCO  4","MOROCCO","AFRICA","25-902-614-8344","BUILDING"

  auto fields = {::arrow::field("C_CUSTKEY", ::arrow::int32()),
				 ::arrow::field("C_NAME", ::arrow::utf8()),
				 ::arrow::field("C_ADDRESS", ::arrow::utf8()),
				 ::arrow::field("C_CITY", ::arrow::utf8()),
				 ::arrow::field("C_NATION", ::arrow::utf8()),
				 ::arrow::field("C_REGION", ::arrow::utf8()),
				 ::arrow::field("C_PHONE", ::arrow::utf8()),
				 ::arrow::field("C_MKTSEGMENT", ::arrow::utf8())};
  auto schema = std::make_shared<::arrow::Schema>(fields);

  return schema;
}

std::shared_ptr<arrow::Schema> SSBSchema::date() {

  // "19920101","January 1, 1992","Thursday","January",1992,199201,"Jan1992",5,1,1,1,1,"Winter","0","0","1","1"

  auto fields = {::arrow::field("D_DATEKEY", ::arrow::utf8()),
				 ::arrow::field("D_DATE", ::arrow::utf8()),
				 ::arrow::field("D_DAYOFWEEK", ::arrow::utf8()),
				 ::arrow::field("D_MONTH", ::arrow::utf8()),
				 ::arrow::field("D_YEAR", ::arrow::int32()),
				 ::arrow::field("D_YEARMONTHNUM", ::arrow::int32()),
				 ::arrow::field("D_YEARMONTH", ::arrow::utf8()),
				 ::arrow::field("D_DAYNUMINWEEK", ::arrow::int32()),
				 ::arrow::field("D_DAYNUMINMONTH", ::arrow::int32()),
				 ::arrow::field("D_DAYNUMINYEAR", ::arrow::int32()),
				 ::arrow::field("D_MONTHNUMINYEAR", ::arrow::int32()),
				 ::arrow::field("D_WEEKNUMINYEAR", ::arrow::int32()),
				 ::arrow::field("D_SELLINGSEASON", ::arrow::utf8()),
				 ::arrow::field("D_LASTDAYINWEEKFL", ::arrow::utf8()),
				 ::arrow::field("D_LASTDAYINMONTHFL", ::arrow::utf8()),
				 ::arrow::field("D_HOLIDAYFL", ::arrow::utf8()),
				 ::arrow::field("D_WEEKDAYFL", ::arrow::utf8())};
  auto schema = std::make_shared<::arrow::Schema>(fields);

  return schema;
}
std::shared_ptr<arrow::Schema> SSBSchema::lineOrder() {

  // 1,1,209,1552,9,"19940925","1-URGENT",0,17,2471035,11507269,4,2372193,87213,2,"19941105","TRUCK"

  auto fields = {::arrow::field("LO_ORDERKEY", ::arrow::int32()),
				 ::arrow::field("LO_LINENUMBER", ::arrow::int32()),
				 ::arrow::field("LO_CUSTKEY", ::arrow::int32()),
				 ::arrow::field("LO_PARTKEY", ::arrow::int32()),
				 ::arrow::field("LO_SUPPKEY", ::arrow::int32()),
				 ::arrow::field("LO_ORDERDATE", ::arrow::utf8()),
				 ::arrow::field("LO_ORDERPRIORITY", ::arrow::utf8()),
				 ::arrow::field("LO_SHIPPRIORITY", ::arrow::int32()),
				 ::arrow::field("LO_QUANTITY", ::arrow::int32()),
				 ::arrow::field("LO_EXTENDEDPRICE", ::arrow::int32()),
				 ::arrow::field("LO_ORDTOTALPRICE", ::arrow::int32()),
				 ::arrow::field("LO_DISCOUNT", ::arrow::int32()),
				 ::arrow::field("LO_REVENUE", ::arrow::int32()),
				 ::arrow::field("LO_SUPPLYCOST", ::arrow::int32()),
				 ::arrow::field("LO_TAX", ::arrow::int32()),
				 ::arrow::field("LO_COMMITDATE", ::arrow::utf8()),
				 ::arrow::field("LO_SHIPMODE", ::arrow::utf8())};

  auto schema = std::make_shared<::arrow::Schema>(fields);

  return schema;
}
std::shared_ptr<arrow::Schema> SSBSchema::part() {

  // 1,"lace spring","MFGR#1","MFGR#11","MFGR#1121","goldenrod","PROMO BURNISHED COPPER",7,"JUMBO PKG"

  auto fields = {::arrow::field("P_PARTKEY", ::arrow::int32()),
				 ::arrow::field("P_NAME", ::arrow::utf8()),
				 ::arrow::field("P_MFGR", ::arrow::utf8()),
				 ::arrow::field("P_CATEGORY", ::arrow::utf8()),
				 ::arrow::field("P_BRAND1", ::arrow::utf8()),
				 ::arrow::field("P_COLOR", ::arrow::utf8()),
				 ::arrow::field("P_TYPE", ::arrow::utf8()),
				 ::arrow::field("P_SIZE", ::arrow::int16()),
				 ::arrow::field("P_CONTAINER", ::arrow::utf8())};

  auto schema = std::make_shared<::arrow::Schema>(fields);

  return schema;
}
std::shared_ptr<arrow::Schema> SSBSchema::supplier() {

  // 1,"Supplier#000000001","sdrGnXCDRcfriBvY0KL,i","PERU     9","PERU","AMERICA","27-989-741-2988"

  auto fields = {::arrow::field("S_SUPPKEY", ::arrow::int32()),
				 ::arrow::field("S_NAME", ::arrow::utf8()),
				 ::arrow::field("S_ADDRESS", ::arrow::utf8()),
				 ::arrow::field("S_CITY", ::arrow::utf8()),
				 ::arrow::field("S_NATION", ::arrow::utf8()),
				 ::arrow::field("S_REGION", ::arrow::utf8()),
				 ::arrow::field("S_PHONE", ::arrow::utf8())};

  auto schema = std::make_shared<::arrow::Schema>(fields);

  return schema;
}
