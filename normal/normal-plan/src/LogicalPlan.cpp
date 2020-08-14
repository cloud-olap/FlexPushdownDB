//
// Created by matt on 16/4/20.
//

#include <normal/plan/LogicalPlan.h>

//#include <graphviz/gvc.h>

using namespace normal::plan;

LogicalPlan::LogicalPlan(std::shared_ptr<std::vector<std::shared_ptr<operator_::LogicalOperator>>> operators)
	: operators_(std::move(operators)) {}

const std::shared_ptr<std::vector<std::shared_ptr<operator_::LogicalOperator>>> &LogicalPlan::getOperators() const {
  return operators_;
}

//void LogicalPlan::writeGraph(const filesystem::path &path) {
//
//  auto gvc = gvContext();
//
//  auto graph = agopen(const_cast<char *>(std::string("Execution Plan").c_str()), Agstrictdirected, 0);
//
//  // Init attributes
//  agattr(graph, AGNODE, const_cast<char *>("fixedsize"), const_cast<char *>("false"));
//  agattr(graph, AGNODE, const_cast<char *>("shape"), const_cast<char *>("ellipse"));
//  agattr(graph, AGNODE, const_cast<char *>("label"), const_cast<char *>("<not set>"));
//  agattr(graph, AGNODE, const_cast<char *>("fontname"), const_cast<char *>("Arial"));
//  agattr(graph, AGNODE, const_cast<char *>("fontsize"), const_cast<char *>("8"));
//
//  // Add all the nodes
//  for (const auto &op: *this->operators_) {
//	std::string nodeName = op->getName();
//	auto node = agnode(graph, const_cast<char *>(nodeName.c_str()), true);
//
//	agset(node, const_cast<char *>("shape"), const_cast<char *>("plaintext"));
//
//	std::string nodeLabel = "<table border='1' cellborder='0' cellpadding='5'>"
//							"<tr><td align='left'><b>" + op->type()->toString() + "</b></td></tr>"
//																				  "<tr><td align='left'>"
//		+ op->getName() + "</td></tr>"
//						  "</table>";
//	char *htmlNodeLabel = agstrdup_html(graph, const_cast<char *>(nodeLabel.c_str()));
//	agset(node, const_cast<char *>("label"), htmlNodeLabel);
//	agstrfree(graph, htmlNodeLabel);
//  }
//
//  // Add all the edges
//  for (const auto &op: *this->operators_) {
//	std::string nodeName = op->getName();
//	auto opNode = agfindnode(graph, (char *)(nodeName.c_str()));
////	for (const auto &c: *op->getConsumer()) {
//	const auto c = op->getConsumer();
//	if(c != nullptr){
//	  auto consumerOpNode = agfindnode(graph, (char *)(c->getName().c_str()));
//	  agedge(graph, opNode, consumerOpNode, const_cast<char *>(std::string("Edge").c_str()), true);
//	}
//  }
//
//  if (!std::experimental::filesystem::exists(path.parent_path())) {
//	throw std::runtime_error(
//		"Could not open file '" + path.string() + "' for writing. Parent directory does not exist");
//  } else {
//	FILE *outFile = fopen(path.c_str(), "w");
//	if (outFile == nullptr) {
//	  throw std::runtime_error(
//		  "Could not open file '" + path.string() + "' for writing. Errno: " + std::to_string(errno));
//	}
//
//	gvLayout(gvc, graph, "dot");
//	gvRender(gvc, graph, "svg", outFile);
//
//	fclose(outFile);
//
//	gvFreeLayout(gvc, graph);
//	agclose(graph);
//	gvFreeContext(gvc);
//  }
//}
