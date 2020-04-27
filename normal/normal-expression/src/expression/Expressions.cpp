//
// Created by matt on 5/4/20.
//

#include <normal/expression/Expressions.h>

using namespace normal::expression;

//std::shared_ptr<arrow::ArrayVector> Expressions::evaluate(const std::shared_ptr<IProjector> &projector,
//														  const arrow::RecordBatch &recordBatch) {
//
//  // Evaluate the expressions
//  auto outputs = std::make_shared<arrow::ArrayVector>();
//  auto status = projector->getGandivaProjector()->Evaluate(recordBatch, arrow::default_memory_pool(), outputs.get());
//
//  if(!status.ok()){
//	throw std::runtime_error(status.message());
//  }
//
//  return outputs;
//}
