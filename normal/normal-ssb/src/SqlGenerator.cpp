//
// Created by Yifei Yang on 8/8/20.
//

#include <normal/ssb/SqlGenerator.h>
#include <random>
#include <iostream>
#include <spdlog/spdlog.h>
#include <cmath>
#include <ctime>

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

  skewWeightQueryNameMap_.emplace("skewWeightQuery1", SkewWeightQuery1);
  skewWeightQueryNameMap_.emplace("skewWeightQuery2", SkewWeightQuery2);
  skewWeightQueryNameMap_.emplace("skewWeightQuery3", SkewWeightQuery3);
  skewWeightQueryNameMap_.emplace("skewWeightQuery4", SkewWeightQuery4);
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

std::string SqlGenerator::generateSql(const std::string& queryName) {
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
  auto lo_discount = genLo_discount(3, 7);
  auto lo_quantity = genLo_quantity(15, 30);
  return fmt::format(
          "select sum(lo_extendedprice * lo_discount) as revenue\n"
          "from lineorder, date\n"
          "where lo_orderdate = d_datekey\n"
          "  and d_year = {}\n"
          "  and (lo_discount between {} and {})\n"
          "  and lo_quantity < {};\n",
          d_year,
          lo_discount - 1,
          lo_discount + 1,
          lo_quantity
          );
}

std::string SqlGenerator::genQuery1_2() {
  auto d_yearmonthnum = genD_yearmonthnum();
  auto lo_discount = genLo_discount(3, 7);
  auto lo_quantity = genLo_quantity(15, 30);
  return fmt::format(
          "select sum(lo_extendedprice * lo_discount) as revenue\n"
          "from lineorder, date\n"
          "where lo_orderdate = d_datekey\n"
          "  and d_yearmonthnum = {}\n"
          "  and (lo_discount between {} and {})\n"
          "  and (lo_quantity between {} and {});\n",
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
  auto lo_discount = genLo_discount(3, 7);
  auto lo_quantity = genLo_quantity(15, 30);
  return fmt::format(
          "select sum(lo_extendedprice * lo_discount) as revenue\n"
          "from lineorder, date\n"
          "where lo_orderdate = d_datekey\n"
          "  and d_weeknuminyear = {}\n"
          "  and d_year = {}\n"
          "  and (lo_discount between {} and {})\n"
          "  and (lo_quantity between {} and {});\n",
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
          "order by d_year, p_brand1;\n",
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
          "order by d_year, p_brand1;\n",
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
          "order by d_year, p_brand1;\n",
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
          "order by d_year asc, revenue desc;\n",
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
          "order by d_year asc, revenue desc;\n",
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
          "order by d_year asc, revenue desc;\n",
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
          "order by d_year asc, revenue desc;\n",
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
          "order by d_year, c_nation;\n",
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
          "order by d_year, s_nation, p_category;\n",
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
          "order by d_year, s_city, p_brand1;\n",
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

std::string SqlGenerator::genD_dayofweek() {
  std::vector<std::string> dayOfWeeks{"Monday", "Tuesday", "Wednesday", "Thursday",
  				      "Friday", "Saturday", "Sunday"};
  std::uniform_int_distribution<int> distribution(0,6);
  int index = distribution(*generator_);
  return dayOfWeeks[index];
}

std::string SqlGenerator::genD_yearmonth() {
  auto year = genD_year();
  std::vector<std::string> months{"Jan", "Feb", "Mar", "Apr", "May", "Jun",
                                  "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"};
  std::uniform_int_distribution<int> distribution(1,12);
  auto month = months[distribution(*generator_) - 1];
  return month + std::to_string(year);
}

int SqlGenerator::genLo_discount(int min, int max) {
  std::uniform_int_distribution<int> distribution(min, max);
  return distribution(*generator_);
}

int SqlGenerator::genLo_quantity(int min, int max) {
  std::uniform_int_distribution<int> distribution(min, max);
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
      int lo_discountFullRange = 10;    // from SSB definition
      int lo_discountRange = lo_discountFullRange * lineorderRowSelectivity_;
      auto lo_discountLeft = genLo_discount(0, lo_discountFullRange - lo_discountRange);
      return "(lo_discount between " + std::to_string(lo_discountLeft) + " and " + std::to_string(lo_discountLeft + lo_discountRange) + ")";
    }
    case 1: {
      int lo_quantityFullRange = 50;    // from SSB definition
      int lo_quantityRange = lo_quantityFullRange * lineorderRowSelectivity_;
      auto lo_quantityLeft = genLo_quantity(0, lo_quantityFullRange - lo_quantityRange);
      return "(lo_quantity between " + std::to_string(lo_quantityLeft) + " and " + std::to_string(lo_quantityLeft + lo_quantityRange) + ")";
    }
    default:
      throw std::runtime_error("Unimplemented lo_predicate kind");
  }
}

/**
 * Generate the predicate on lineorder's orderdate
 * using Zipfian distribution on 8 partitions of orderdate values
 */
std::vector<double> zipfian(int n, double alpha) {
  std::vector<double> possibilities;
  double baseDen = 0.0;
  for (double i = 1.0; i <= n; i++) {
    baseDen += pow(1 / i, alpha);
  }
  for (int i = 1; i <= n; i++) {
    possibilities.emplace_back(1.0 / (pow(i, alpha) * baseDen));
  }
  return possibilities;
}

std::vector<std::string> SqlGenerator::generateSqlBatchSkew(float skewness, int batchSize) {
  // collect all skew query names
  std::vector<std::string> skewQueryNames;
  for (auto const &queryNameIt: skewQueryNameMap_) {
    skewQueryNames.emplace_back(queryNameIt.first);
  }

  const bool useZipfian = true;

  // simple skewness
  const double hotPercentage = 0.2;

  // zipfian skewness
  const int n = 7;
  const auto alpha = skewness;
  auto possibilities = zipfian(n, alpha);
  std::uniform_real_distribution<double> distribution1(0.0, 1.0);

  // random queries
  std::uniform_int_distribution<int> distribution2(0,skewQueryNames.size() - 1);

  std::vector<std::string> queries;

  for (size_t kind = 0; kind < possibilities.size(); kind++) {
    auto numThisKind = round(possibilities[kind] * ((double) batchSize));
    std::string skewLo_predicate = fmt::format("(lo_orderdate between {} and {})", (1992 + kind) * 10000 + 101,
            (1992 + kind) * 10000 + 1231);

    for (int i = 0; i < numThisKind; i++) {
      int skewQueryIndex = distribution2(*generator_);
      auto skewQueryName = skewQueryNames[skewQueryIndex];
      queries.emplace_back(generateSqlSkew(skewQueryName, skewLo_predicate));
    }
  }

  if (queries.size() == batchSize) {
    return queries;
  } else if (queries.size() > batchSize) {
    return std::vector<std::string>(queries.begin(), queries.begin() + batchSize);
  } else {
    while (queries.size() < batchSize) {
      std::string skewLo_predicate = fmt::format("(lo_orderdate between {} and {})", (1992) * 10000 + 101,
              (1992) * 10000 + 1231);
      int skewQueryIndex = distribution2(*generator_);
      auto skewQueryName = skewQueryNames[skewQueryIndex];
      queries.emplace_back(generateSqlSkew(skewQueryName, skewLo_predicate));
    }
    return queries;
  }
}

std::string SqlGenerator::generateSqlSkew(std::string queryName, std::string skewLo_predicate) {
  enum SkewQueryName queryNameEnum;
  auto queryNameIt = skewQueryNameMap_.find(queryName);
  if (queryNameIt == skewQueryNameMap_.end()) {
    queryNameEnum = SkewUnknown;
  } else {
    queryNameEnum = queryNameIt->second;
  }

  auto lo_predicate = genLo_predicate();
  switch (queryNameEnum) {
    case SkewQuery2_1: return genSkewQuery2_1(skewLo_predicate, lo_predicate, "lo_revenue");
    case SkewQuery2_2: return genSkewQuery2_2(skewLo_predicate, lo_predicate, "lo_revenue");
    case SkewQuery2_3: return genSkewQuery2_3(skewLo_predicate, lo_predicate, "lo_revenue");
    case SkewQuery3_1: return genSkewQuery3_1(skewLo_predicate, lo_predicate, "lo_revenue");
    case SkewQuery3_2: return genSkewQuery3_2(skewLo_predicate, lo_predicate, "lo_revenue");
    case SkewQuery3_3: return genSkewQuery3_3(skewLo_predicate, lo_predicate, "lo_revenue");
    case SkewQuery3_4: return genSkewQuery3_4(skewLo_predicate, lo_predicate, "lo_revenue");
    case SkewQuery4_1: return genSkewQuery4_1(skewLo_predicate, lo_predicate, "lo_revenue - lo_supplycost");
    case SkewQuery4_2: return genSkewQuery4_2(skewLo_predicate, lo_predicate, "lo_revenue - lo_supplycost");
    case SkewQuery4_3: return genSkewQuery4_3(skewLo_predicate, lo_predicate, "lo_revenue - lo_supplycost"  );
    default:
      throw std::runtime_error("Unknown skew query name: " + queryName);
  }
}

std::string SqlGenerator::genSkewQuery2_1(std::string skewLo_predicate, std::string lo_predicate, std::string aggColumn) {
  auto p_category = "MFGR#" + std::to_string(genP_category_num());
  auto s_region = genS_region();
  return fmt::format(
          "select sum({}), d_yearmonthnum, p_brand1\n"
          "from lineorder, date, part, supplier\n"
          "where lo_orderdate = d_datekey\n"
          "  and lo_partkey = p_partkey\n"
          "  and lo_suppkey = s_suppkey\n"
          "  and p_category = '{}'\n"
          "  and s_region = '{}'\n"
          "  and {}\n"
          "  and {}\n"
          "group by d_yearmonthnum, p_brand1\n"
          "order by d_yearmonthnum, p_brand1;\n",
          aggColumn,
          p_category,
          s_region,
          lo_predicate,
          skewLo_predicate
  );
}

std::string SqlGenerator::genSkewQuery2_2(std::string skewLo_predicate, std::string lo_predicate, std::string aggColumn) {
  auto p_brand1_num = genP_brand1_num();
  auto p_brand1_0 = "MFGR#" + std::to_string(p_brand1_num - 3);
  auto p_brand1_1 = "MFGR#" + std::to_string(p_brand1_num + 4);
  auto s_region = genS_region();
  return fmt::format(
          "select sum({}), d_yearmonthnum, p_brand1\n"
          "from lineorder, date, part, supplier\n"
          "where lo_orderdate = d_datekey\n"
          "  and lo_partkey = p_partkey\n"
          "  and lo_suppkey = s_suppkey\n"
          "  and (p_brand1 between '{}' and '{}')\n"
          "  and s_region = '{}'\n"
          "  and {}\n"
          "  and {}\n"
          "group by d_yearmonthnum, p_brand1\n"
          "order by d_yearmonthnum, p_brand1;\n",
          aggColumn,
          p_brand1_0,
          p_brand1_1,
          s_region,
          lo_predicate,
          skewLo_predicate
  );
}

std::string SqlGenerator::genSkewQuery2_3(std::string skewLo_predicate, std::string lo_predicate, std::string aggColumn) {
  auto p_brand1 = "MFGR#" + std::to_string(genP_brand1_num());
  auto s_region = genS_region();
  return fmt::format(
          "select sum({}), d_yearmonthnum, p_brand1\n"
          "from lineorder, date, part, supplier\n"
          "where lo_orderdate = d_datekey\n"
          "  and lo_partkey = p_partkey\n"
          "  and lo_suppkey = s_suppkey\n"
          "  and p_brand1 = '{}'\n"
          "  and s_region = '{}'\n"
          "  and {}\n"
          "  and {}\n"
          "group by d_yearmonthnum, p_brand1\n"
          "order by d_yearmonthnum, p_brand1;\n",
          aggColumn,
          p_brand1,
          s_region,
          lo_predicate,
          skewLo_predicate
  );
}

std::string SqlGenerator::genSkewQuery3_1(std::string skewLo_predicate, std::string lo_predicate, std::string aggColumn) {
  auto region = genS_region();
  return fmt::format(
          "select c_nation, s_nation, d_yearmonthnum, sum({0}) as revenue\n"
          "from customer, lineorder, supplier, date\n"
          "where lo_custkey = c_custkey\n"
          "  and lo_suppkey = s_suppkey\n"
          "  and lo_orderdate = d_datekey\n"
          "  and c_region = '{1}'\n"
          "  and s_region = '{1}'\n"
          "  and {2}\n"
          "  and {3}\n"
          "group by c_nation, s_nation, d_yearmonthnum\n"
          "order by d_yearmonthnum asc, revenue desc;\n",
          aggColumn,
          region,
          lo_predicate,
          skewLo_predicate
  );
}

std::string SqlGenerator::genSkewQuery3_2(std::string skewLo_predicate, std::string lo_predicate, std::string aggColumn) {
  auto nation = genS_nation();
  return fmt::format(
          "select c_city, s_city, d_yearmonthnum, sum({0}) as revenue\n"
          "from customer, lineorder, supplier, date\n"
          "where lo_custkey = c_custkey\n"
          "  and lo_suppkey = s_suppkey\n"
          "  and lo_orderdate = d_datekey\n"
          "  and c_nation = '{1}'\n"
          "  and s_nation = '{1}'\n"
          "  and {2}\n"
          "  and {3}\n"
          "group by c_city, s_city, d_yearmonthnum\n"
          "order by d_yearmonthnum asc, revenue desc;\n",
          aggColumn,
          nation,
          lo_predicate,
          skewLo_predicate
  );
}

std::string SqlGenerator::genSkewQuery3_3(std::string skewLo_predicate, std::string lo_predicate, std::string aggColumn) {
  auto city1 = genS_city();
  auto city2 = genS_city();
  return fmt::format(
          "select c_city, s_city, d_yearmonthnum, sum({0}) as revenue\n"
          "from customer, lineorder, supplier, date\n"
          "where lo_custkey = c_custkey\n"
          "  and lo_suppkey = s_suppkey\n"
          "  and lo_orderdate = d_datekey\n"
          "  and (c_city = '{1}' or c_city = '{2}')\n"
          "  and (s_city = '{1}' or s_city = '{2}')\n"
          "  and {3}\n"
          "  and {4}\n"
          "group by c_city, s_city, d_yearmonthnum\n"
          "order by d_yearmonthnum asc, revenue desc;\n",
          aggColumn,
          city1,
          city2,
          lo_predicate,
          skewLo_predicate
  );
}

std::string SqlGenerator::genSkewQuery3_4(std::string skewLo_predicate, std::string lo_predicate, std::string aggColumn) {
  auto city1 = genS_city();
  auto city2 = genS_city();
  return fmt::format(
          "select c_city, s_city, d_yearmonthnum, sum({0}) as revenue\n"
          "from customer, lineorder, supplier, date\n"
          "where lo_custkey = c_custkey\n"
          "  and lo_suppkey = s_suppkey\n"
          "  and lo_orderdate = d_datekey\n"
          "  and (c_city = '{1}' or c_city = '{2}')\n"
          "  and (s_city = '{1}' or s_city = '{2}')\n"
          "  and {3}\n"
          "  and {4}\n"
          "group by c_city, s_city, d_yearmonthnum\n"
          "order by d_yearmonthnum asc, revenue desc;\n",
          aggColumn,
          city1,
          city2,
          lo_predicate,
          skewLo_predicate
  );
}

std::string SqlGenerator::genSkewQuery4_1(std::string skewLo_predicate, std::string lo_predicate, std::string aggColumn) {
  auto region = genS_region();
  auto p_mfgr1 = "MFGR#" + std::to_string(genP_mfgr_num());
  auto p_mfgr2 = "MFGR#" + std::to_string(genP_mfgr_num());
  return fmt::format(
          "select d_yearmonthnum, c_nation, sum({0}) as profit\n"
          "from date, customer, supplier, part, lineorder\n"
          "where lo_custkey = c_custkey\n"
          "  and lo_suppkey = s_suppkey\n"
          "  and lo_partkey = p_partkey\n"
          "  and lo_orderdate = d_datekey\n"
          "  and c_region = '{1}'\n"
          "  and s_region = '{1}'\n"
          "  and (p_mfgr = '{2}' or p_mfgr = '{3}')\n"
          "  and {4}\n"
          "  and {5}\n"
          "group by d_yearmonthnum, c_nation\n"
          "order by d_yearmonthnum, c_nation;\n",
          aggColumn,
          region,
          p_mfgr1,
          p_mfgr2,
          lo_predicate,
          skewLo_predicate
  );
}

std::string SqlGenerator::genSkewQuery4_2(std::string skewLo_predicate, std::string lo_predicate, std::string aggColumn) {
  auto region = genS_region();
  auto p_mfgr1 = "MFGR#" + std::to_string(genP_mfgr_num());
  auto p_mfgr2 = "MFGR#" + std::to_string(genP_mfgr_num());
  return fmt::format(
          "select d_yearmonthnum, s_nation, p_category, sum({0}) as profit\n"
          "from date, customer, supplier, part, lineorder\n"
          "where lo_custkey = c_custkey\n"
          "  and lo_suppkey = s_suppkey\n"
          "  and lo_partkey = p_partkey\n"
          "  and lo_orderdate = d_datekey\n"
          "  and c_region = '{1}'\n"
          "  and s_region = '{1}'\n"
          "  and (p_mfgr = '{2}' or p_mfgr = '{3}')\n"
          "  and {4}\n"
          "  and {5}\n"
          "group by d_yearmonthnum, s_nation, p_category\n"
          "order by d_yearmonthnum, s_nation, p_category;\n",
          aggColumn,
          region,
          p_mfgr1,
          p_mfgr2,
          lo_predicate,
          skewLo_predicate
  );
}

std::string SqlGenerator::genSkewQuery4_3(std::string skewLo_predicate, std::string lo_predicate, std::string aggColumn) {
  auto s_nation = genS_nation();
  auto p_category = "MFGR#" + std::to_string(genP_category_num());
  return fmt::format(
          "select d_yearmonthnum, s_city, p_brand1, sum({}) as profit\n"
          "from date, customer, supplier, part, lineorder\n"
          "where lo_custkey = c_custkey\n"
          "  and lo_suppkey = s_suppkey\n"
          "  and lo_partkey = p_partkey\n"
          "  and lo_orderdate = d_datekey\n"
          "  and s_nation = '{}'\n"
          "  and p_category = '{}'\n"
          "  and {}\n"
          "  and {}\n"
          "group by d_yearmonthnum, s_city, p_brand1\n"
          "order by d_yearmonthnum, s_city, p_brand1;\n",
          aggColumn,
          s_nation,
          p_category,
          lo_predicate,
          skewLo_predicate
  );
}

std::vector<std::string> SqlGenerator::generateSqlBatchSkewWeight(float skewness, int batchSize) {
  // collect all skew query names
  std::vector<std::string> skewWeightQueryNames;
  for (auto const &queryNameIt: skewWeightQueryNameMap_) {
    skewWeightQueryNames.emplace_back(queryNameIt.first);
  }

  // zipfian parameters
  const int n = 7;
  const auto alpha = skewness;
  auto possibilities = zipfian(n, alpha);
  std::uniform_real_distribution<double> distribution1(0.0, 1.0);

  // random queries
  std::uniform_int_distribution<int> distribution2(1, skewWeightQueryNames.size() - 1);

  std::vector<std::string> queries;

  for (size_t kind = 0; kind < possibilities.size(); kind++) {
    auto numThisKind = round(possibilities[kind] * ((double) batchSize));
    std::string skewLo_predicate = fmt::format("(lo_orderdate between {} and {})", (1992 + kind) * 10000 + 101,
                                               (1992 + kind) * 10000 + 1231);

    for (int i = 0; i < numThisKind; i++) {
      int skewWeightQueryIndex = distribution2(*generator_);
      auto skewWeightQueryName = skewWeightQueryNames[skewWeightQueryIndex];
      queries.emplace_back(generateSqlSkewWeight(skewWeightQueryName, skewLo_predicate, i % 2 == 0));
    }
  }

  if (queries.size() == batchSize) {
    return queries;
  } else if (queries.size() > batchSize) {
    return std::vector<std::string>(queries.begin(), queries.begin() + batchSize);
  } else {
    while (queries.size() < batchSize) {
      std::string skewLo_predicate = fmt::format("(lo_orderdate between {} and {})", (1993) * 10000 + 101,
                                                 (1993) * 10000 + 1231);
      int skewWeightQueryIndex = distribution2(*generator_);
      auto skewWeightQueryName = skewWeightQueryNames[skewWeightQueryIndex];
      queries.emplace_back(generateSqlSkewWeight(skewWeightQueryName, skewLo_predicate, true));
    }
    return queries;
  }
}

std::string SqlGenerator::generateSqlSkewWeight(std::string queryName, std::string skewLo_predicate, bool high) {
  enum SkewWeightQueryName queryNameEnum;
  auto queryNameIt = skewWeightQueryNameMap_.find(queryName);
  if (queryNameIt == skewWeightQueryNameMap_.end()) {
    queryNameEnum = SkewWeightUnknown;
  } else {
    queryNameEnum = queryNameIt->second;
  }

  auto lo_predicate = genLo_predicate(high);
  std::string aggColumn = high? "sum(lo_revenue), sum(lo_supplycost), sum(lo_linenumber)" : "sum(lo_extendedprice), sum(lo_ordtotalprice)";

  switch (queryNameEnum) {
    case SkewWeightQuery1: return genSkewWeightQuery1(skewLo_predicate, lo_predicate, aggColumn);
    case SkewWeightQuery2: return genSkewWeightQuery2(skewLo_predicate, lo_predicate, aggColumn);
    case SkewWeightQuery3: return genSkewWeightQuery3(skewLo_predicate, lo_predicate, aggColumn);
    case SkewWeightQuery4: return genSkewWeightQuery4(skewLo_predicate, lo_predicate, aggColumn);
    default:
      throw std::runtime_error("Unknown skew query name: " + queryName);
  }
}

std::string SqlGenerator::genLo_predicate(bool high) {
  if (high) {
    std::uniform_int_distribution<int> distribution(15,35);
    auto quantity = distribution(*generator_);
    return "lo_quantity = " + std::to_string(quantity);
  } else {
    std::uniform_int_distribution<int> distribution(3,6);
    auto lowDiscount = distribution(*generator_);
    auto highDiscount = lowDiscount + 1;
    return fmt::format("(lo_discount < {} or lo_discount > {})",
                        lowDiscount,
                        highDiscount);
  }
}


std::string SqlGenerator::genSkewWeightQuery1(std::string skewLo_predicate, std::string lo_predicate, std::string aggColumn) {
  /*auto dayOfWeek = genD_dayofweek();
  return fmt::format(
          "select d_yearmonthnum, {}\n"
          "from date, lineorder\n"
          "where lo_orderdate = d_datekey\n"
          "  and d_dayofweek = '{}'\n"
          "  and {}\n"
          "  and {}\n"
          "group by d_yearmonthnum;\n",
          aggColumn,
          dayOfWeek,
          lo_predicate,
          skewLo_predicate
  );*/
  auto s_city = genS_city();
  return fmt::format(
	  "select s_name, {}\n"
	  "from supplier, lineorder\n"
	  "where lo_suppkey = s_suppkey\n"
	  "  and s_city = '{}'\n"
	  "  and {}\n"
	  "  and {}\n"
	  "group by s_name;\n",
	  aggColumn,
	  s_city,
	  lo_predicate,
	  skewLo_predicate
  );
}

std::string SqlGenerator::genSkewWeightQuery2(std::string skewLo_predicate, std::string lo_predicate, std::string aggColumn) {
  auto s_city = genS_city();
  return fmt::format(
          "select s_name, {}\n"
          "from supplier, lineorder\n"
          "where lo_suppkey = s_suppkey\n"
          "  and s_city = '{}'\n"
          "  and {}\n"
          "  and {}\n"
          "group by s_name;\n",
          aggColumn,
          s_city,
          lo_predicate,
          skewLo_predicate
  );
  //return genSkewWeightQuery1(skewLo_predicate, lo_predicate, aggColumn);
}

std::string SqlGenerator::genSkewWeightQuery3(std::string skewLo_predicate, std::string lo_predicate, std::string aggColumn) {
  auto c_city = genS_city();
  return fmt::format(
          "select c_name, {}\n"
          "from customer, lineorder\n"
          "where lo_custkey = c_custkey\n"
          "  and c_city = '{}'\n"
          "  and {}\n"
          "  and {}\n"
          "group by c_name;\n",
          aggColumn,
          c_city,
          lo_predicate,
          skewLo_predicate
  );
  //return genSkewWeightQuery1(skewLo_predicate, lo_predicate, aggColumn);
}

std::string SqlGenerator::genSkewWeightQuery4(std::string skewLo_predicate, std::string lo_predicate, std::string aggColumn) {
  auto p_brand1 = "MFGR#" + std::to_string(genP_brand1_num());
  return fmt::format(
          "select p_size, {}\n"
          "from part, lineorder\n"
          "where lo_partkey = p_partkey\n"
          "  and p_brand1 = '{}'\n"
          "  and {}\n"
          "  and {}\n"
          "group by p_size;\n",
          aggColumn,
          p_brand1,
          lo_predicate,
          skewLo_predicate
  );
  //return genSkewWeightQuery1(skewLo_predicate, lo_predicate, aggColumn);
}

std::vector<std::string> SqlGenerator::generateSqlBatchSkewRecurring(float skewness) {
  std::vector<std::string> queryNames{"skewQuery2_1", "skewQuery2_2", "skewQuery2_3",
                                      "skewQuery3_1", "skewQuery3_2", "skewQuery3_3", "skewQuery3_4",
                                      "skewQuery4_1", "skewQuery4_2", "skewQuery4_3"};
  std::vector<std::string> queries;
  const auto batchSize = queryNames.size();

  // Generate total 10 skewLo_predicates
  const int n = 7;
  auto possibilities = zipfian(n, skewness);
  std::vector<std::string> skewLo_predicates;

  for (size_t kind = 0; kind < possibilities.size(); kind++) {
    auto numThisKind = round(possibilities[kind] * ((double) batchSize));
    std::string skewLo_predicate = fmt::format("(lo_orderdate between {} and {})", (1992 + kind) * 10000 + 101,
                                               (1992 + kind) * 10000 + 1231);
    for (int i = 0; i < numThisKind; i++) {
      skewLo_predicates.emplace_back(skewLo_predicate);
    }
  }

  while (skewLo_predicates.size() > queryNames.size()) {
    skewLo_predicates.erase(skewLo_predicates.begin());
  }

  while (skewLo_predicates.size() < queryNames.size()) {
    skewLo_predicates.emplace_back("(lo_orderdate between 19930101 and 19931231)");
  }

  std::random_device rd;
  std::mt19937 g(rd());
  std::shuffle(skewLo_predicates.begin(), skewLo_predicates.end(), g);

  // Generate queries
  for (int i = 0; i < queryNames.size(); i++) {
    queries.emplace_back(generateSqlSkew(queryNames[i], skewLo_predicates[i]));
  }

  return queries;
}

std::vector<std::string> SqlGenerator::generateSqlBatchHotQuery(float percentage, int batchSize) {
  std::vector<std::string> queries;

  // Collect all skew query names
  std::vector<std::string> skewQueryNames;
  for (auto const &queryNameIt: skewQueryNameMap_) {
    skewQueryNames.emplace_back(queryNameIt.first);
  }

  // The hot query
  const int n = 7;
  std::uniform_int_distribution<int> distribution1(0, n - 1);
  std::uniform_int_distribution<int> distribution2(0,skewQueryNames.size() - 1);
  int skewLo_predicateIndex = distribution1(*generator_);
  std::string skewLo_predicate = fmt::format("(lo_orderdate between {} and {})", (1992 + skewLo_predicateIndex) * 10000 + 101,
                                             (1992 + skewLo_predicateIndex) * 10000 + 1231);
  int skewQueryIndex = distribution2(*generator_);
  auto skewQueryName = skewQueryNames[skewQueryIndex];

  std::string hotQuery = generateSqlSkew(skewQueryName, skewLo_predicate);
  for (int i = 0; i < (int)(batchSize*percentage); i++) {
    queries.emplace_back(hotQuery);
  }

  // Rest in the uniform distribution
  auto restNum = batchSize - (int)(batchSize*percentage);
  for (int i = 0; i < restNum; i++) {
    skewLo_predicateIndex = i % n;
    skewLo_predicate = fmt::format("(lo_orderdate between {} and {})", (1992 + skewLo_predicateIndex) * 10000 + 101,
                                   (1992 + skewLo_predicateIndex) * 10000 + 1231);
    skewQueryIndex = distribution2(*generator_);
    skewQueryName = skewQueryNames[skewQueryIndex];
    queries.emplace_back(generateSqlSkew(skewQueryName, skewLo_predicate));
  }

  return queries;
}

// Adjust date by a number of days +/-
void DatePlusDays( struct tm* date, int days )
{
  const time_t ONE_DAY = 24 * 60 * 60 ;

  // Seconds since start of epoch
  time_t date_seconds = mktime( date ) + (days * ONE_DAY) ;

  // Update caller's date
  // Use localtime because mktime converts to UTC so may change date
  *date = *localtime( &date_seconds ) ; ;
}

std::vector<std::string> SqlGenerator::generateSqlForMathModel(double hitRatio, double rowPer, int nCol) {
  std::vector<std::string> lineOrderColumns =
          {"lo_orderkey", "lo_linenumber", "lo_custkey", "lo_partkey", "lo_suppkey", "lo_orderdate",
           "lo_orderpriority", "lo_shippriority", "lo_quantity", "lo_extendedprice", "lo_ordtotalprice",
           "lo_discount", "lo_revenue", "lo_supplycost", "lo_tax", "lo_commitdate", "lo_shipmode"};
  std::string columns;
  for (int i = 0; i < nCol; i++) {
    columns += lineOrderColumns[i] + ", ";
  }
  columns = columns.substr(0, columns.length() - 2);

  // make use of k
  std::string predicate = "lo_quantity <= " + std::to_string((int) (50 * rowPer));
  std::cout << "Predicate: " << predicate << std::endl;

  // make use of hit ratio
  int days = (int) (365.0 * 2 * hitRatio);
  tm date = tm();
  date.tm_mday = 1;
  date.tm_mon = 1 - 1;
  date.tm_year = 1992 - 1900;
  DatePlusDays(&date, days);
  int endDate = (date.tm_year + 1900) * 10000 + (date.tm_mon + 1) * 100 + date.tm_mday;
  std::cout << "End date: " << endDate << std::endl;

  // query for caching
  std::string sql1 = fmt::format(
          "select {}\n"
          "from lineorder\n"
          "where ({})"
          "  and (lo_orderdate between 19920101 and {});\n",
          columns,
          predicate,
          endDate
  );

  // query for measurement
  std::string sql2 = fmt::format(
          "select {}\n"
          "from lineorder\n"
          "where ({})"
          "  and (lo_orderdate between 19920101 and 19931231);\n",
          columns,
          predicate
  );

  // query for measurement without local processing
  std::string sql3 = fmt::format(
          "select {}\n"
          "from lineorder\n"
          "where (lo_orderdate between 19920101 and 19931231);\n",
          columns
  );

  std::cout << sql1 << std::endl;
  std::cout << sql2 << std::endl;
  std::cout << sql3 << std::endl;
  return std::vector<std::string>{sql1, sql2, sql3};
}

void SqlGenerator::setLineorderRowSelectivity(double lineorderRowSelectivity) {
  lineorderRowSelectivity_ = lineorderRowSelectivity;
}
