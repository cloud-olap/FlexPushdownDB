//
// Created by Yifei Yang on 7/15/20.
//

#include <string>
#include <normal/connector/MiniCatalogue.h>

normal::connector::MiniCatalogue::MiniCatalogue(const std::shared_ptr<std::vector<std::string>> &tables,
              const std::shared_ptr<std::unordered_map<std::string, std::shared_ptr<std::vector<std::string>>>> &schemas,
              const std::shared_ptr<std::vector<std::string>> &defaultJoinOrder) :
        tables_(tables),
        schemas_(schemas),
        defaultJoinOrder_(defaultJoinOrder) {}

std::shared_ptr<normal::connector::MiniCatalogue> normal::connector::MiniCatalogue::defaultMiniCatalogue() {
  // star join order
  auto defaultJoinOrder = std::make_shared<std::vector<std::string>>();
  defaultJoinOrder->emplace_back("supplier");
  defaultJoinOrder->emplace_back("date");
  defaultJoinOrder->emplace_back("customer");
  defaultJoinOrder->emplace_back("part");

  // schemas
  auto cols_supplier = std::vector<std::string>{"s_suppkey", "s_name", "s_address", "s_city", "s_nation", "s_region",
                                                "s_phone"};
  auto cols_date = std::vector<std::string>{"d_datekey", "d_date", "d_dayofweek", "d_month", "d_year", "d_yearmonthnum",
                                            "d_yearmonth", "d_daynuminweek", "d_daynuminmonth", "d_monthnuminyear",
                                            "d_weeknuminyear", "d_sellingseason", "d_lastdayinmonthfl", "d_holidayfl",
                                            "d_weekdayfl", "d_daynuminyear"};
  auto cols_customer = std::vector<std::string>{"c_custkey", "c_name", "c_address", "c_city", "c_nation", "c_region",
                                                "c_phone", "c_mktsegment"};
  auto cols_part = std::vector<std::string>{"p_partkey", "p_name", "p_mfgr", "p_category", "p_brand1", "p_color",
                                            "p_type", "p_size", "p_container"};
  auto cols_lineorder = std::vector<std::string>{"lo_orderkey", "lo_linenumber", "lo_custkey", "lo_partkey",
                                                 "lo_suppkey", "lo_orderdate", "lo_ordpriority", "lo_shippriority",
                                                 "lo_quantity", "lo_extendedprice", "lo_ordtotalprice", "lo_discount",
                                                 "lo_revenue", "lo_supplycost", "lo_tax", "lo_commitdate",
                                                 "lo_shipmode"};
  auto schemas = std::make_shared<std::unordered_map<std::string, std::shared_ptr<std::vector<std::string>>>>();
  schemas->insert({"supplier", std::make_shared<std::vector<std::string>>(cols_supplier)});
  schemas->insert({"date", std::make_shared<std::vector<std::string>>(cols_date)});
  schemas->insert({"customer", std::make_shared<std::vector<std::string>>(cols_customer)});
  schemas->insert({"part", std::make_shared<std::vector<std::string>>(cols_part)});
  schemas->insert({"lineorder", std::make_shared<std::vector<std::string>>(cols_lineorder)});

  // tables
  auto tables = std::make_shared<std::vector<std::string>>();
  for (const auto &schema: *schemas) {
    tables->push_back(schema.first);
  }

  return std::make_shared<MiniCatalogue>(MiniCatalogue(tables, schemas, defaultJoinOrder));
}

std::shared_ptr<std::vector<std::string>> normal::connector::MiniCatalogue::tables() {
  return tables_;
}

std::shared_ptr<std::vector<std::string>> normal::connector::MiniCatalogue::defaultJoinOrder() {
  return defaultJoinOrder_;
}

std::shared_ptr<std::unordered_map<std::string, std::shared_ptr<std::vector<std::string>>>>
normal::connector::MiniCatalogue::schemas() {
  return schemas_;
}

std::string normal::connector::MiniCatalogue::findTableOfColumn(std::string columnName) {
  for (const auto &schema: *schemas_) {
    for (const auto &existColumnName: *(schema.second)) {
      if (existColumnName == columnName) {
        return schema.first;
      }
    }
  }
  throw std::runtime_error("Column " + columnName + " not found");
}