//
// Created by Yifei Yang on 7/15/20.
//

#include <string>
#include "normal/plan/MiniCatalogue.h"

void normal::plan::MiniCatalogue::init() {
  // star join order
  defaultJoinOrder->insert({"supplier", 1});
  defaultJoinOrder->insert({"date", 2});
  defaultJoinOrder->insert({"customer", 3});
  defaultJoinOrder->insert({"part", 4});

  // schemas
  auto cols_supplier = std::vector<std::string>{"s_suppkey", "s_name", "s_address", "s_city", "s_nation", "s_region",
                                                "s_phone"};
  auto cols_date = std::vector<std::string>{"d_datekey", "d_date", "d_dayofweek", "d_month", "d_year", "d_yearmonthnum",
                                            "d_yearmonth", "d_daynuminweek", "d_daynuminmonth", "d_monthnuminyear",
                                            "d_weeknuminyear", "d_sellingseason", "d_lastdayinmonthfl", "d_holidayfl",
                                            "d_weekdayfl", "d_daynuminyear"};
  auto cols_customer = std::vector<std::string>{"c_custkey", "c_name", "c_address", "c_city", "c_nation", "c_region",
                                                "c_phone", "c_mktsegment"};
  auto cols_part = std::vector<std::string>{"p_partkey", "p_name", "p_mgfr", "p_category", "p_brand1", "p_color",
                                            "p_type", "p_size", "p_container"};
  auto cols_lineorder = std::vector<std::string>{"lo_orderkey", "lo_linenumber", "lo_custkey", "lo_partkey",
                                                 "lo_suppkey", "lo_orderdate", "lo_ordpriority", "lo_shippriority",
                                                 "lo_quantity", "lo_extendedprice", "lo_ordtotalprice", "lo_discount",
                                                 "lo_revenue", "lo_supplycost", "lo_tax", "lo_commitdate",
                                                 "lo_shipmode"};
  schemas->insert({"supplier", std::shared_ptr<std::vector<std::string>>(&cols_supplier)});
  schemas->insert({"date", std::shared_ptr<std::vector<std::string>>(&cols_date)});
  schemas->insert({"customer", std::shared_ptr<std::vector<std::string>>(&cols_customer)});
  schemas->insert({"part", std::shared_ptr<std::vector<std::string>>(&cols_part)});
  schemas->insert({"lineorder", std::shared_ptr<std::vector<std::string>>(&cols_lineorder)});
}
