//
// Created by matt on 10/8/20.
//

#include "normal/ssb/SSBSchema.h"

#include <normal/tuple/ColumnName.h>

using namespace normal::tuple;

std::shared_ptr<arrow::Schema> SSBSchema::customer() {

  // 1,"Customer#000000001","j5JsirBM9P","MOROCCO  4","MOROCCO","AFRICA","25-902-614-8344","BUILDING"

  auto fields = {::arrow::field(ColumnName::canonicalize("C_CUSTKEY"), ::arrow::int32()),
				 ::arrow::field(ColumnName::canonicalize("C_NAME"), ::arrow::utf8()),
				 ::arrow::field(ColumnName::canonicalize("C_ADDRESS"), ::arrow::utf8()),
				 ::arrow::field(ColumnName::canonicalize("C_CITY"), ::arrow::utf8()),
				 ::arrow::field(ColumnName::canonicalize("C_NATION"), ::arrow::utf8()),
				 ::arrow::field(ColumnName::canonicalize("C_REGION"), ::arrow::utf8()),
				 ::arrow::field(ColumnName::canonicalize("C_PHONE"), ::arrow::utf8()),
				 ::arrow::field(ColumnName::canonicalize("C_MKTSEGMENT"), ::arrow::utf8())};
  auto schema = std::make_shared<::arrow::Schema>(fields);

  return schema;
}

std::shared_ptr<arrow::Schema> SSBSchema::date() {

  // "19920101","January 1, 1992","Thursday","January",1992,199201,"Jan1992",5,1,1,1,1,"Winter","0","0","1","1"

  auto fields = {::arrow::field(ColumnName::canonicalize("D_DATEKEY"), ::arrow::int32()),
				 ::arrow::field(ColumnName::canonicalize("D_DATE"), ::arrow::utf8()),
				 ::arrow::field(ColumnName::canonicalize("D_DAYOFWEEK"), ::arrow::utf8()),
				 ::arrow::field(ColumnName::canonicalize("D_MONTH"), ::arrow::utf8()),
				 ::arrow::field(ColumnName::canonicalize("D_YEAR"), ::arrow::int32()),
				 ::arrow::field(ColumnName::canonicalize("D_YEARMONTHNUM"), ::arrow::int32()),
				 ::arrow::field(ColumnName::canonicalize("D_YEARMONTH"), ::arrow::utf8()),
				 ::arrow::field(ColumnName::canonicalize("D_DAYNUMINWEEK"), ::arrow::int32()),
				 ::arrow::field(ColumnName::canonicalize("D_DAYNUMINMONTH"), ::arrow::int32()),
				 ::arrow::field(ColumnName::canonicalize("D_DAYNUMINYEAR"), ::arrow::int32()),
				 ::arrow::field(ColumnName::canonicalize("D_MONTHNUMINYEAR"), ::arrow::int32()),
				 ::arrow::field(ColumnName::canonicalize("D_WEEKNUMINYEAR"), ::arrow::int32()),
				 ::arrow::field(ColumnName::canonicalize("D_SELLINGSEASON"), ::arrow::utf8()),
				 ::arrow::field(ColumnName::canonicalize("D_LASTDAYINWEEKFL"), ::arrow::boolean()),
				 ::arrow::field(ColumnName::canonicalize("D_LASTDAYINMONTHFL"), ::arrow::boolean()),
				 ::arrow::field(ColumnName::canonicalize("D_HOLIDAYFL"), ::arrow::boolean()),
				 ::arrow::field(ColumnName::canonicalize("D_WEEKDAYFL"), ::arrow::boolean())};
  auto schema = std::make_shared<::arrow::Schema>(fields);

  return schema;
}
std::shared_ptr<arrow::Schema> SSBSchema::lineOrder() {

  // 1,1,209,1552,9,"19940925","1-URGENT",0,17,2471035,11507269,4,2372193,87213,2,"19941105","TRUCK"

  auto fields = {::arrow::field(ColumnName::canonicalize("LO_ORDERKEY"), ::arrow::int32()),
				 ::arrow::field(ColumnName::canonicalize("LO_LINENUMBER"), ::arrow::int32()),
				 ::arrow::field(ColumnName::canonicalize("LO_CUSTKEY"), ::arrow::int32()),
				 ::arrow::field(ColumnName::canonicalize("LO_PARTKEY"), ::arrow::int32()),
				 ::arrow::field(ColumnName::canonicalize("LO_SUPPKEY"), ::arrow::int32()),
				 ::arrow::field(ColumnName::canonicalize("LO_ORDERDATE"), ::arrow::int32()),
				 ::arrow::field(ColumnName::canonicalize("LO_ORDERPRIORITY"), ::arrow::utf8()),
				 ::arrow::field(ColumnName::canonicalize("LO_SHIPPRIORITY"), ::arrow::int32()),
				 ::arrow::field(ColumnName::canonicalize("LO_QUANTITY"), ::arrow::int32()),
				 ::arrow::field(ColumnName::canonicalize("LO_EXTENDEDPRICE"), ::arrow::int32()),
				 ::arrow::field(ColumnName::canonicalize("LO_ORDTOTALPRICE"), ::arrow::int32()),
				 ::arrow::field(ColumnName::canonicalize("LO_DISCOUNT"), ::arrow::int32()),
				 ::arrow::field(ColumnName::canonicalize("LO_REVENUE"), ::arrow::int64()),
				 ::arrow::field(ColumnName::canonicalize("LO_SUPPLYCOST"), ::arrow::int64()),
				 ::arrow::field(ColumnName::canonicalize("LO_TAX"), ::arrow::int32()),
				 ::arrow::field(ColumnName::canonicalize("LO_COMMITDATE"), ::arrow::int32()),
				 ::arrow::field(ColumnName::canonicalize("LO_SHIPMODE"), ::arrow::utf8())};

  auto schema = std::make_shared<::arrow::Schema>(fields);

  return schema;
}
std::shared_ptr<arrow::Schema> SSBSchema::part() {

  // 1,"lace spring","MFGR#1","MFGR#11","MFGR#1121","goldenrod","PROMO BURNISHED COPPER",7,"JUMBO PKG"

  auto fields = {::arrow::field(ColumnName::canonicalize("P_PARTKEY"), ::arrow::int32()),
				 ::arrow::field(ColumnName::canonicalize("P_NAME"), ::arrow::utf8()),
				 ::arrow::field(ColumnName::canonicalize("P_MFGR"), ::arrow::utf8()),
				 ::arrow::field(ColumnName::canonicalize("P_CATEGORY"), ::arrow::utf8()),
				 ::arrow::field(ColumnName::canonicalize("P_BRAND1"), ::arrow::utf8()),
				 ::arrow::field(ColumnName::canonicalize("P_COLOR"), ::arrow::utf8()),
				 ::arrow::field(ColumnName::canonicalize("P_TYPE"), ::arrow::utf8()),
				 ::arrow::field(ColumnName::canonicalize("P_SIZE"), ::arrow::int32()),
				 ::arrow::field(ColumnName::canonicalize("P_CONTAINER"), ::arrow::utf8())};

  auto schema = std::make_shared<::arrow::Schema>(fields);

  return schema;
}
std::shared_ptr<arrow::Schema> SSBSchema::supplier() {

  // 1,"Supplier#000000001","sdrGnXCDRcfriBvY0KL,i","PERU     9","PERU","AMERICA","27-989-741-2988"

  auto fields = {::arrow::field(ColumnName::canonicalize("S_SUPPKEY"), ::arrow::int32()),
				 ::arrow::field(ColumnName::canonicalize("S_NAME"), ::arrow::utf8()),
				 ::arrow::field(ColumnName::canonicalize("S_ADDRESS"), ::arrow::utf8()),
				 ::arrow::field(ColumnName::canonicalize("S_CITY"), ::arrow::utf8()),
				 ::arrow::field(ColumnName::canonicalize("S_NATION"), ::arrow::utf8()),
				 ::arrow::field(ColumnName::canonicalize("S_REGION"), ::arrow::utf8()),
				 ::arrow::field(ColumnName::canonicalize("S_PHONE"), ::arrow::utf8())};

  auto schema = std::make_shared<::arrow::Schema>(fields);

  return schema;
}
