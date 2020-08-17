//
// Created by Yifei Yang on 8/8/20.
//

#include <normal/ssb/SqlGenerator.h>
#include <random>
#include <iostream>
#include <spdlog/spdlog.h>

using namespace normal::ssb;

SqlGenerator::SqlGenerator() {
  std::random_device rd;
  generator_ = std::make_shared<std::default_random_engine>(rd());

  queryNameMap_.emplace("query1_1", Query1_1);
  queryNameMap_.emplace("query1_2", Query1_2);
  queryNameMap_.emplace("query1_3", Query1_3);
  queryNameMap_.emplace("query2_1", Query2_1);
  queryNameMap_.emplace("query2_2", Query2_2);
  queryNameMap_.emplace("query2_3", Query2_3);
  queryNameMap_.emplace("query3_1", Query3_1);
  queryNameMap_.emplace("query3_2", Query3_2);
  queryNameMap_.emplace("query3_3", Query3_3);
  queryNameMap_.emplace("query3_4", Query3_4);
  queryNameMap_.emplace("query4_1", Query4_1);
  queryNameMap_.emplace("query4_2", Query4_2);
  queryNameMap_.emplace("query4_3", Query4_3);


  skewQueryNameMap_.emplace("skewQuery2_1", SkewQuery2_1);
  skewQueryNameMap_.emplace("skewQuery2_2", SkewQuery2_2);
  skewQueryNameMap_.emplace("skewQuery2_3", SkewQuery2_3);
  skewQueryNameMap_.emplace("skewQuery3_1", SkewQuery3_1);
  skewQueryNameMap_.emplace("skewQuery3_2", SkewQuery3_2);
  skewQueryNameMap_.emplace("skewQuery3_3", SkewQuery3_3);
  skewQueryNameMap_.emplace("skewQuery3_4", SkewQuery3_4);
  skewQueryNameMap_.emplace("skewQuery4_1", SkewQuery4_1);
  skewQueryNameMap_.emplace("skewQuery4_2", SkewQuery4_2);
  skewQueryNameMap_.emplace("skewQuery4_3", SkewQuery4_3);
}

std::vector<std::string> SqlGenerator::generateSqlBatch(int batchSize) {
  // collect all query names
  std::vector<std::string> queryNames;
  for (auto const &queryNameIt: queryNameMap_) {
    queryNames.emplace_back(queryNameIt.first);
  }
  // generate randomly
  std::vector<std::string> queries;
  std::uniform_int_distribution<int> distribution(0,queryNames.size() - 1);
  auto dice = std::bind(distribution, *generator_);
  for (int i = 0; i < batchSize; ++i) {
    int queryIndex = dice();
    auto queryName = queryNames[queryIndex];
    queries.emplace_back(generateSql(queryName));
  }
  return queries;
}

std::string SqlGenerator::generateSql(std::string queryName) {
  enum QueryName queryNameEnum;
  auto queryNameIt = queryNameMap_.find(queryName);
  if (queryNameIt == queryNameMap_.end()) {
    queryNameEnum = Unknown;
  } else {
    queryNameEnum = queryNameIt->second;
  }

  switch (queryNameEnum) {
    case Query1_1: return genQuery1_1();
    case Query1_2: return genQuery1_2();
    case Query1_3: return genQuery1_3();
    case Query2_1: return genQuery2_1();
    case Query2_2: return genQuery2_2();
    case Query2_3: return genQuery2_3();
    case Query3_1: return genQuery3_1();
    case Query3_2: return genQuery3_2();
    case Query3_3: return genQuery3_3();
    case Query3_4: return genQuery3_4();
    case Query4_1: return genQuery4_1();
    case Query4_2: return genQuery4_2();
    case Query4_3: return genQuery4_3();
    default:
      throw std::runtime_error("Unknown query name: " + queryName);
  }
}

std::string SqlGenerator::genQuery1_1() {
  auto d_year = genD_year();
  auto lo_discount = genLo_discount();
  auto lo_quantity = genLo_quantity();
  return fmt::format(
          "select sum(lo_extendedprice * lo_discount) as revenue\n"
          "from lineorder, date\n"
          "where lo_orderdate = d_datekey\n"
          "  and d_year = {}\n"
          "  and (lo_discount between {} and {})\n"
          "  and lo_quantity < {};",
          d_year,
          lo_discount - 1,
          lo_discount + 1,
          lo_quantity
          );
}

std::string SqlGenerator::genQuery1_2() {
  auto d_yearmonthnum = genD_yearmonthnum();
  auto lo_discount = genLo_discount();
  auto lo_quantity = genLo_quantity();
  return fmt::format(
          "select sum(lo_extendedprice * lo_discount) as revenue\n"
          "from lineorder, date\n"
          "where lo_orderdate = d_datekey\n"
          "  and d_yearmonthnum = {}\n"
          "  and (lo_discount between {} and {})\n"
          "  and (lo_quantity between {} and {});",
          d_yearmonthnum,
          lo_discount - 1,
          lo_discount + 1,
          lo_quantity - 4,
          lo_quantity + 5
          );
}

std::string SqlGenerator::genQuery1_3() {
  auto d_weeknuminyear = genD_weeknuminyear();
  auto d_year = genD_year();
  auto lo_discount = genLo_discount();
  auto lo_quantity = genLo_quantity();
  return fmt::format(
          "select sum(lo_extendedprice * lo_discount) as revenue\n"
          "from lineorder, date\n"
          "where lo_orderdate = d_datekey\n"
          "  and d_weeknuminyear = {}\n"
          "  and d_year = {}\n"
          "  and (lo_discount between {} and {})\n"
          "  and (lo_quantity between {} and {});",
          d_weeknuminyear,
          d_year,
          lo_discount - 1,
          lo_discount + 1,
          lo_quantity - 4,
          lo_quantity + 5
  );
}

std::string SqlGenerator::genQuery2_1() {
  auto p_category = "MFGR#" + std::to_string(genP_category_num());
  auto s_region = genS_region();
  auto lo_predicate = genLo_predicate();
  return fmt::format(
          "select sum(lo_revenue), d_year, p_brand1\n"
          "from lineorder, date, part, supplier\n"
          "where lo_orderdate = d_datekey\n"
          "  and lo_partkey = p_partkey\n"
          "  and lo_suppkey = s_suppkey\n"
          "  and p_category = '{}'\n"
          "  and s_region = '{}'\n"
          "  and {}\n"
          "group by d_year, p_brand1\n"
          "order by d_year, p_brand1;",
          p_category,
          s_region,
          lo_predicate
          );
}

std::string SqlGenerator::genQuery2_2() {
  auto p_brand1_num = genP_brand1_num();
  auto p_brand1_0 = "MFGR#" + std::to_string(p_brand1_num - 3);
  auto p_brand1_1 = "MFGR#" + std::to_string(p_brand1_num + 4);
  auto s_region = genS_region();
  auto lo_predicate = genLo_predicate();
  return fmt::format(
          "select sum(lo_revenue), d_year, p_brand1\n"
          "from lineorder, date, part, supplier\n"
          "where lo_orderdate = d_datekey\n"
          "  and lo_partkey = p_partkey\n"
          "  and lo_suppkey = s_suppkey\n"
          "  and (p_brand1 between '{}' and '{}')\n"
          "  and s_region = '{}'\n"
          "  and {}\n"
          "group by d_year, p_brand1\n"
          "order by d_year, p_brand1;",
          p_brand1_0,
          p_brand1_1,
          s_region,
          lo_predicate
          );
}

std::string SqlGenerator::genQuery2_3() {
  auto p_brand1 = "MFGR#" + std::to_string(genP_brand1_num());
  auto s_region = genS_region();
  auto lo_predicate = genLo_predicate();
  return fmt::format(
          "select sum(lo_revenue), d_year, p_brand1\n"
          "from lineorder, date, part, supplier\n"
          "where lo_orderdate = d_datekey\n"
          "  and lo_partkey = p_partkey\n"
          "  and lo_suppkey = s_suppkey\n"
          "  and p_brand1 = '{}'\n"
          "  and s_region = '{}'\n"
          "  and {}\n"
          "group by d_year, p_brand1\n"
          "order by d_year, p_brand1;",
          p_brand1,
          s_region,
          lo_predicate
          );
}

std::string SqlGenerator::genQuery3_1() {
  auto region = genS_region();
  auto d_year1 = genD_year();
  auto d_year2 = genD_year();
  auto d_year_pair = (d_year1 <= d_year2) ? std::make_pair(d_year1, d_year2) : std::make_pair(d_year2, d_year1);
  auto lo_predicate = genLo_predicate();
  return fmt::format(
          "select c_nation, s_nation, d_year, sum(lo_revenue) as revenue\n"
          "from customer, lineorder, supplier, date\n"
          "where lo_custkey = c_custkey\n"
          "  and lo_suppkey = s_suppkey\n"
          "  and lo_orderdate = d_datekey\n"
          "  and c_region = '{0}'\n"
          "  and s_region = '{0}'\n"
          "  and d_year >= {1}\n"
          "  and d_year <= {2}\n"
          "  and {3}\n"
          "group by c_nation, s_nation, d_year\n"
          "order by d_year asc, revenue desc;",
          region,
          d_year_pair.first,
          d_year_pair.second,
          lo_predicate
          );
}

std::string SqlGenerator::genQuery3_2() {
  auto nation = genS_nation();
  auto d_year1 = genD_year();
  auto d_year2 = genD_year();
  auto d_year_pair = (d_year1 <= d_year2) ? std::make_pair(d_year1, d_year2) : std::make_pair(d_year2, d_year1);
  auto lo_predicate = genLo_predicate();
  return fmt::format(
          "select c_city, s_city, d_year, sum(lo_revenue) as revenue\n"
          "from customer, lineorder, supplier, date\n"
          "where lo_custkey = c_custkey\n"
          "  and lo_suppkey = s_suppkey\n"
          "  and lo_orderdate = d_datekey\n"
          "  and c_nation = '{0}'\n"
          "  and s_nation = '{0}'\n"
          "  and d_year >= {1}\n"
          "  and d_year <= {2}\n"
          "  and {3}\n"
          "group by c_city, s_city, d_year\n"
          "order by d_year asc, revenue desc;",
          nation,
          d_year_pair.first,
          d_year_pair.second,
          lo_predicate
  );
}

std::string SqlGenerator::genQuery3_3() {
  auto city1 = genS_city();
  auto city2 = genS_city();
  auto d_year1 = genD_year();
  auto d_year2 = genD_year();
  auto d_year_pair = (d_year1 <= d_year2) ? std::make_pair(d_year1, d_year2) : std::make_pair(d_year2, d_year1);
  auto lo_predicate = genLo_predicate();
  return fmt::format(
          "select c_city, s_city, d_year, sum(lo_revenue) as revenue\n"
          "from customer, lineorder, supplier, date\n"
          "where lo_custkey = c_custkey\n"
          "  and lo_suppkey = s_suppkey\n"
          "  and lo_orderdate = d_datekey\n"
          "  and (c_city = '{0}' or c_city = '{1}')\n"
          "  and (s_city = '{0}' or s_city = '{1}')\n"
          "  and d_year >= {2}\n"
          "  and d_year <= {3}\n"
          "  and {4}\n"
          "group by c_city, s_city, d_year\n"
          "order by d_year asc, revenue desc;",
          city1,
          city2,
          d_year_pair.first,
          d_year_pair.second,
          lo_predicate
  );
}

std::string SqlGenerator::genQuery3_4() {
  auto city1 = genS_city();
  auto city2 = genS_city();
  auto d_yearmonth = genD_yearmonth();
  auto lo_predicate = genLo_predicate();
  return fmt::format(
          "select c_city, s_city, d_year, sum(lo_revenue) as revenue\n"
          "from customer, lineorder, supplier, date\n"
          "where lo_custkey = c_custkey\n"
          "  and lo_suppkey = s_suppkey\n"
          "  and lo_orderdate = d_datekey\n"
          "  and (c_city = '{0}' or c_city = '{1}')\n"
          "  and (s_city = '{0}' or s_city = '{1}')\n"
          "  and d_yearmonth = '{2}'\n"
          "  and {3}\n"
          "group by c_city, s_city, d_year\n"
          "order by d_year asc, revenue desc;",
          city1,
          city2,
          d_yearmonth,
          lo_predicate
  );
}

std::string SqlGenerator::genQuery4_1() {
  auto region = genS_region();
  auto p_mfgr1 = "MFGR#" + std::to_string(genP_mfgr_num());
  auto p_mfgr2 = "MFGR#" + std::to_string(genP_mfgr_num());
  auto lo_predicate = genLo_predicate();
  return fmt::format(
          "select d_year, c_nation, sum(lo_revenue - lo_supplycost) as profit\n"
          "from date, customer, supplier, part, lineorder\n"
          "where lo_custkey = c_custkey\n"
          "  and lo_suppkey = s_suppkey\n"
          "  and lo_partkey = p_partkey\n"
          "  and lo_orderdate = d_datekey\n"
          "  and c_region = '{0}'\n"
          "  and s_region = '{0}'\n"
          "  and (p_mfgr = '{1}' or p_mfgr = '{2}')\n"
          "  and {3}\n"
          "group by d_year, c_nation\n"
          "order by d_year, c_nation;",
          region,
          p_mfgr1,
          p_mfgr2,
          lo_predicate
          );
}

std::string SqlGenerator::genQuery4_2() {
  auto region = genS_region();
  auto d_year1 = genD_year();
  auto d_year2 = genD_year();
  auto p_mfgr1 = "MFGR#" + std::to_string(genP_mfgr_num());
  auto p_mfgr2 = "MFGR#" + std::to_string(genP_mfgr_num());
  auto lo_predicate = genLo_predicate();
  return fmt::format(
          "select d_year, s_nation, p_category, sum(lo_revenue - lo_supplycost) as profit\n"
          "from date, customer, supplier, part, lineorder\n"
          "where lo_custkey = c_custkey\n"
          "  and lo_suppkey = s_suppkey\n"
          "  and lo_partkey = p_partkey\n"
          "  and lo_orderdate = d_datekey\n"
          "  and c_region = '{0}'\n"
          "  and s_region = '{0}'\n"
          "  and (d_year = {1} or d_year = {2})\n"
          "  and (p_mfgr = '{3}' or p_mfgr = '{4}')\n"
          "  and {5}\n"
          "group by d_year, s_nation, p_category\n"
          "order by d_year, s_nation, p_category;",
          region,
          d_year1,
          d_year2,
          p_mfgr1,
          p_mfgr2,
          lo_predicate
  );
}

std::string SqlGenerator::genQuery4_3() {
  auto s_nation = genS_nation();
  auto d_year1 = genD_year();
  auto d_year2 = genD_year();
  auto p_category = "MFGR#" + std::to_string(genP_category_num());
  auto lo_predicate = genLo_predicate();
  return fmt::format(
          "select d_year, s_city, p_brand1, sum(lo_revenue - lo_supplycost) as profit\n"
          "from date, customer, supplier, part, lineorder\n"
          "where lo_custkey = c_custkey\n"
          "  and lo_suppkey = s_suppkey\n"
          "  and lo_partkey = p_partkey\n"
          "  and lo_orderdate = d_datekey\n"
          "  and s_nation = '{}'\n"
          "  and (d_year = {} or d_year = {})\n"
          "  and p_category = '{}'\n"
          "  and {}\n"
          "group by d_year, s_city, p_brand1\n"
          "order by d_year, s_city, p_brand1;",
          s_nation,
          d_year1,
          d_year2,
          p_category,
          lo_predicate
  );
}

int SqlGenerator::genD_year() {
  std::uniform_int_distribution<int> distribution(1992,1998);
  return distribution(*generator_);
}

int SqlGenerator::genD_yearmonthnum() {
  auto year = genD_year();
  std::uniform_int_distribution<int> distribution(1,12);
  auto month = distribution(*generator_);
  return year * 100 + month;
}

int SqlGenerator::genD_weeknuminyear() {
  std::uniform_int_distribution<int> distribution(10,40);
  return distribution(*generator_);
}

std::string SqlGenerator::genD_yearmonth() {
  auto year = genD_year();
  std::vector<std::string> months{"Jan", "Feb", "Mar", "Apr", "May", "Jun",
                                  "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"};
  std::uniform_int_distribution<int> distribution(1,12);
  auto month = months[distribution(*generator_) - 1];
  return month + std::to_string(year);
}

int SqlGenerator::genLo_discount() {
  std::uniform_int_distribution<int> distribution(3,7);
  return distribution(*generator_);
}

int SqlGenerator::genLo_quantity() {
  std::uniform_int_distribution<int> distribution(15,30);
  return distribution(*generator_);
}

int SqlGenerator::genP_category_num() {
  std::uniform_int_distribution<int> distribution(1,5);
  auto tenth = distribution(*generator_);
  auto unit = distribution(*generator_);
  return tenth * 10 + unit;
}

int SqlGenerator::genP_brand1_num() {
  auto category_num = genP_category_num();
  std::uniform_int_distribution<int> distribution(10,30);
  auto brand1_num = distribution(*generator_);
  return category_num * 100 + brand1_num;
}

int SqlGenerator::genP_mfgr_num() {
  std::uniform_int_distribution<int> distribution(1,5);
  return distribution(*generator_);
}

std::string SqlGenerator::genS_region() {
  std::vector<std::string> regions{"AMERICA", "AFRICA", "MIDDLE EAST", "EUROPE", "ASIA"};
  std::uniform_int_distribution<int> distribution(0,4);
  return regions[distribution(*generator_)];
}

std::string SqlGenerator::genS_nation() {
  std::vector<std::string> nations{"UNITED STATES", "CHINA", "UNITED KINGDOM", "INDIA", "RUSSIA"};
  std::uniform_int_distribution<int> distribution(0,4);
  return nations[distribution(*generator_)];
}

std::string SqlGenerator::genS_city() {
  const size_t indent = 9;
  std::vector<std::string> nationsPrefix{"UNITED ST", "CHINA", "UNITED KI", "INDIA", "RUSSIA"};
  std::uniform_int_distribution<int> distribution1(0,4);
  auto nation = nationsPrefix[distribution1(*generator_)];
  for (auto i = nation.length(); i < indent; ++i) {
    nation += " ";
  }

  std::uniform_int_distribution<int> distribution2(0,9);
  auto city_num = distribution2(*generator_);
  return nation + std::to_string(city_num);
}

std::string SqlGenerator::genLo_predicate() {
  const int numKinds = 2;
  std::uniform_int_distribution<int> distribution(1, numKinds);
  auto kind = distribution(*generator_);
  switch (kind % numKinds) {
    case 0: {
      auto lo_discount = genLo_discount();
      return "lo_discount between " + std::to_string(lo_discount - 1) + " and " + std::to_string(lo_discount + 1);
    }
    case 1: {
      auto lo_quantity = genLo_quantity();
      return "lo_quantity between " + std::to_string(lo_quantity - 4) + " and " + std::to_string(lo_quantity + 5);
    }
    default:
      throw std::runtime_error("Unimplemented lo_predicate kind");
  }
}

/**
 * Generate the predicate on lineorder's orderdate: 80% 19911231-19930523; 20% 19930524-19981231
 */
std::vector<std::string> SqlGenerator::generateSqlBatchSkew(int batchSize) {
  // collect all skew query names
  std::vector<std::string> skewQueryNames;
  for (auto const &queryNameIt: skewQueryNameMap_) {
    skewQueryNames.emplace_back(queryNameIt.first);
  }

  // generate randomly
  const int hotPercentage = 20;
  std::uniform_int_distribution<int> distribution1(1, 100);
  std::uniform_int_distribution<int> distribution2(0,skewQueryNames.size() - 1);

  std::vector<std::string> queries;
  for (int i = 0; i < batchSize; i++) {
    std::string lo_predicate;
    int kind = distribution1(*generator_);
    if (kind <= hotPercentage) {
      lo_predicate = fmt::format("lo_orderdate between {} and {}", 19911231, 19930523);
    } else {
      lo_predicate = fmt::format("lo_orderdate between {} and {}", 19930524, 19981231);
    }

    int skewQueryIndex = distribution2(*generator_);
    auto skewQueryName = skewQueryNames[skewQueryIndex];
    queries.emplace_back(generateSqlSkew(skewQueryName, lo_predicate));
  }

  return queries;
}

std::string SqlGenerator::generateSqlSkew(std::string queryName, std::string lo_predicate) {
  enum SkewQueryName queryNameEnum;
  auto queryNameIt = skewQueryNameMap_.find(queryName);
  if (queryNameIt == skewQueryNameMap_.end()) {
    queryNameEnum = SkewUnknown;
  } else {
    queryNameEnum = queryNameIt->second;
  }

  switch (queryNameEnum) {
    case SkewQuery2_1: return genSkewQuery2_1(lo_predicate);
    case SkewQuery2_2: return genSkewQuery2_2(lo_predicate);
    case SkewQuery2_3: return genSkewQuery2_3(lo_predicate);
    case SkewQuery3_1: return genSkewQuery3_1(lo_predicate);
    case SkewQuery3_2: return genSkewQuery3_2(lo_predicate);
    case SkewQuery3_3: return genSkewQuery3_3(lo_predicate);
    case SkewQuery3_4: return genSkewQuery3_4(lo_predicate);
    case SkewQuery4_1: return genSkewQuery4_1(lo_predicate);
    case SkewQuery4_2: return genSkewQuery4_2(lo_predicate);
    case SkewQuery4_3: return genSkewQuery4_3(lo_predicate);
    default:
      throw std::runtime_error("Unknown skew query name: " + queryName);
  }
}

std::string SqlGenerator::genSkewQuery2_1(std::string lo_predicate) {
  auto p_category = "MFGR#" + std::to_string(genP_category_num());
  auto s_region = genS_region();
  return fmt::format(
          "select sum(lo_revenue), d_year, p_brand1\n"
          "from lineorder, date, part, supplier\n"
          "where lo_orderdate = d_datekey\n"
          "  and lo_partkey = p_partkey\n"
          "  and lo_suppkey = s_suppkey\n"
          "  and p_category = '{}'\n"
          "  and s_region = '{}'\n"
          "  and {}\n"
          "group by d_year, p_brand1\n"
          "order by d_year, p_brand1;",
          p_category,
          s_region,
          lo_predicate
  );
}

std::string SqlGenerator::genSkewQuery2_2(std::string lo_predicate) {
  auto p_brand1_num = genP_brand1_num();
  auto p_brand1_0 = "MFGR#" + std::to_string(p_brand1_num - 3);
  auto p_brand1_1 = "MFGR#" + std::to_string(p_brand1_num + 4);
  auto s_region = genS_region();
  return fmt::format(
          "select sum(lo_revenue), d_year, p_brand1\n"
          "from lineorder, date, part, supplier\n"
          "where lo_orderdate = d_datekey\n"
          "  and lo_partkey = p_partkey\n"
          "  and lo_suppkey = s_suppkey\n"
          "  and (p_brand1 between '{}' and '{}')\n"
          "  and s_region = '{}'\n"
          "  and {}\n"
          "group by d_year, p_brand1\n"
          "order by d_year, p_brand1;",
          p_brand1_0,
          p_brand1_1,
          s_region,
          lo_predicate
  );
}

std::string SqlGenerator::genSkewQuery2_3(std::string lo_predicate) {
  auto p_brand1 = "MFGR#" + std::to_string(genP_brand1_num());
  auto s_region = genS_region();
  return fmt::format(
          "select sum(lo_revenue), d_year, p_brand1\n"
          "from lineorder, date, part, supplier\n"
          "where lo_orderdate = d_datekey\n"
          "  and lo_partkey = p_partkey\n"
          "  and lo_suppkey = s_suppkey\n"
          "  and p_brand1 = '{}'\n"
          "  and s_region = '{}'\n"
          "  and {}\n"
          "group by d_year, p_brand1\n"
          "order by d_year, p_brand1;",
          p_brand1,
          s_region,
          lo_predicate
  );
}

std::string SqlGenerator::genSkewQuery3_1(std::string lo_predicate) {
  auto region = genS_region();
  return fmt::format(
          "select c_nation, s_nation, d_year, sum(lo_revenue) as revenue\n"
          "from customer, lineorder, supplier, date\n"
          "where lo_custkey = c_custkey\n"
          "  and lo_suppkey = s_suppkey\n"
          "  and lo_orderdate = d_datekey\n"
          "  and c_region = '{0}'\n"
          "  and s_region = '{0}'\n"
          "  and {1}\n"
          "group by c_nation, s_nation, d_year\n"
          "order by d_year asc, revenue desc;",
          region,
          lo_predicate
  );
}

std::string SqlGenerator::genSkewQuery3_2(std::string lo_predicate) {
  auto nation = genS_nation();
  return fmt::format(
          "select c_city, s_city, d_year, sum(lo_revenue) as revenue\n"
          "from customer, lineorder, supplier, date\n"
          "where lo_custkey = c_custkey\n"
          "  and lo_suppkey = s_suppkey\n"
          "  and lo_orderdate = d_datekey\n"
          "  and c_nation = '{0}'\n"
          "  and s_nation = '{0}'\n"
          "  and {1}\n"
          "group by c_city, s_city, d_year\n"
          "order by d_year asc, revenue desc;",
          nation,
          lo_predicate
  );
}

std::string SqlGenerator::genSkewQuery3_3(std::string lo_predicate) {
  auto city1 = genS_city();
  auto city2 = genS_city();
  return fmt::format(
          "select c_city, s_city, d_year, sum(lo_revenue) as revenue\n"
          "from customer, lineorder, supplier, date\n"
          "where lo_custkey = c_custkey\n"
          "  and lo_suppkey = s_suppkey\n"
          "  and lo_orderdate = d_datekey\n"
          "  and (c_city = '{0}' or c_city = '{1}')\n"
          "  and (s_city = '{0}' or s_city = '{1}')\n"
          "  and {2}\n"
          "group by c_city, s_city, d_year\n"
          "order by d_year asc, revenue desc;",
          city1,
          city2,
          lo_predicate
  );
}

std::string SqlGenerator::genSkewQuery3_4(std::string lo_predicate) {
  auto city1 = genS_city();
  auto city2 = genS_city();
  return fmt::format(
          "select c_city, s_city, d_year, sum(lo_revenue) as revenue\n"
          "from customer, lineorder, supplier, date\n"
          "where lo_custkey = c_custkey\n"
          "  and lo_suppkey = s_suppkey\n"
          "  and lo_orderdate = d_datekey\n"
          "  and (c_city = '{0}' or c_city = '{1}')\n"
          "  and (s_city = '{0}' or s_city = '{1}')\n"
          "  and {2}\n"
          "group by c_city, s_city, d_year\n"
          "order by d_year asc, revenue desc;",
          city1,
          city2,
          lo_predicate
  );
}

std::string SqlGenerator::genSkewQuery4_1(std::string lo_predicate) {
  auto region = genS_region();
  auto p_mfgr1 = "MFGR#" + std::to_string(genP_mfgr_num());
  auto p_mfgr2 = "MFGR#" + std::to_string(genP_mfgr_num());
  return fmt::format(
          "select d_year, c_nation, sum(lo_revenue - lo_supplycost) as profit\n"
          "from date, customer, supplier, part, lineorder\n"
          "where lo_custkey = c_custkey\n"
          "  and lo_suppkey = s_suppkey\n"
          "  and lo_partkey = p_partkey\n"
          "  and lo_orderdate = d_datekey\n"
          "  and c_region = '{0}'\n"
          "  and s_region = '{0}'\n"
          "  and (p_mfgr = '{1}' or p_mfgr = '{2}')\n"
          "  and {3}\n"
          "group by d_year, c_nation\n"
          "order by d_year, c_nation;",
          region,
          p_mfgr1,
          p_mfgr2,
          lo_predicate
  );
}

std::string SqlGenerator::genSkewQuery4_2(std::string lo_predicate) {
  auto region = genS_region();
  auto p_mfgr1 = "MFGR#" + std::to_string(genP_mfgr_num());
  auto p_mfgr2 = "MFGR#" + std::to_string(genP_mfgr_num());
  return fmt::format(
          "select d_year, s_nation, p_category, sum(lo_revenue - lo_supplycost) as profit\n"
          "from date, customer, supplier, part, lineorder\n"
          "where lo_custkey = c_custkey\n"
          "  and lo_suppkey = s_suppkey\n"
          "  and lo_partkey = p_partkey\n"
          "  and lo_orderdate = d_datekey\n"
          "  and c_region = '{0}'\n"
          "  and s_region = '{0}'\n"
          "  and (p_mfgr = '{1}' or p_mfgr = '{2}')\n"
          "  and {3}\n"
          "group by d_year, s_nation, p_category\n"
          "order by d_year, s_nation, p_category;",
          region,
          p_mfgr1,
          p_mfgr2,
          lo_predicate
  );
}

std::string SqlGenerator::genSkewQuery4_3(std::string lo_predicate) {
  auto s_nation = genS_nation();
  auto p_category = "MFGR#" + std::to_string(genP_category_num());
  return fmt::format(
          "select d_year, s_city, p_brand1, sum(lo_revenue - lo_supplycost) as profit\n"
          "from date, customer, supplier, part, lineorder\n"
          "where lo_custkey = c_custkey\n"
          "  and lo_suppkey = s_suppkey\n"
          "  and lo_partkey = p_partkey\n"
          "  and lo_orderdate = d_datekey\n"
          "  and s_nation = '{}'\n"
          "  and p_category = '{}'\n"
          "  and {}\n"
          "group by d_year, s_city, p_brand1\n"
          "order by d_year, s_city, p_brand1;",
          s_nation,
          p_category,
          lo_predicate
  );
}

