//
// Created by matt on 10/8/20.
//
//
//#include "normal/ssb/query2_1/Operators.h"
//
//#include <experimental/filesystem>
//
//#include <normal/ssb/Globals.h>
//#include <normal/expression/gandiva/Column.h>
//#include <normal/core/type/Float64Type.h>
//#include <normal/core/graph/OperatorGraph.h>
//#include <normal/expression/gandiva/StringLiteral.h>
//#include <normal/expression/gandiva/EqualTo.h>
//#include <normal/pushdown/Util.h>
//
//#include <normal/connector/local-fs/LocalFilePartition.h>
//
//using namespace normal::ssb::query2_1;
//using namespace std::experimental;
//using namespace normal::pushdown::aggregate;
//using namespace normal::core::type;
//using namespace normal::core::graph;
//using namespace normal::expression::gandiva;
//
//std::vector<std::shared_ptr<normal::pushdown::filter::Filter>>
//Operators::makeSupplierFilterOperators(const std::string &region,
//									   int numConcurrentUnits,
//									   const std::shared_ptr<OperatorGraph> &g) {
//
//  std::vector<std::shared_ptr<normal::pushdown::filter::Filter>> os;
//  for (int u = 0; u < numConcurrentUnits; ++u) {
//	auto o = normal::pushdown::filter::Filter::make(
//		fmt::format("/query-{}/supplier-filter-{}", g->getId(), u),
//		FilterPredicate::make(
//			eq(col("s_region"), str_lit(region))));
//	g->put(o);
//	os.push_back(o);
//  }
//
//  return os;
//}
//
//std::vector<std::shared_ptr<normal::pushdown::filter::Filter>>
//Operators::makePartFilterOperators(const std::string &category,
//								   int numConcurrentUnits,
//								   const std::shared_ptr<OperatorGraph> &g) {
//
//  std::vector<std::shared_ptr<normal::pushdown::filter::Filter>> os;
//  for (int u = 0; u < numConcurrentUnits; ++u) {
//	auto o = normal::pushdown::filter::Filter::make(
//		fmt::format("/query-{}/part-filter-{}", g->getId(), u),
//		FilterPredicate::make(
//			eq(col("p_category"), str_lit(category))));
//	g->put(o);
//	os.push_back(o);
//  }
//
//  return os;
//}
