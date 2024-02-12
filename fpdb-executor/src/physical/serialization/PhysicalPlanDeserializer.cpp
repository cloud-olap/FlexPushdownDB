//
// Created by Yifei Yang on 2/21/22.
//

#include <fpdb/executor/physical/serialization/PhysicalPlanDeserializer.h>
#include <fpdb/executor/physical/fpdb-store/FPDBStoreFileScanPOp.h>
#include <fpdb/executor/physical/filter/FilterPOp.h>
#include <fpdb/executor/physical/project/ProjectPOp.h>
#include <fpdb/executor/physical/aggregate/AggregatePOp.h>
#include <fpdb/executor/physical/group/GroupPOp.h>
#include <fpdb/executor/physical/shuffle/ShufflePOp.h>
#include <fpdb/executor/physical/bloomfilter/BloomFilterCreatePOp.h>
#include <fpdb/executor/physical/bloomfilter/BloomFilterUsePOp.h>
#include <fpdb/executor/physical/join/hashjoin/HashJoinArrowPOp.h>
#include <fpdb/executor/physical/collate/CollatePOp.h>
#include <fpdb/executor/physical/transform/PrePToPTransformerUtil.h>
#include <fpdb/tuple/serialization/ArrowSerializer.h>

using json = ::nlohmann::json;

namespace fpdb::executor::physical {

PhysicalPlanDeserializer::PhysicalPlanDeserializer(const std::string &planString,
                                                   const std::string &storeRootPath):
  planString_(planString),
  storeRootPath_(storeRootPath) {}

tl::expected<std::shared_ptr<PhysicalPlan>, std::string>
PhysicalPlanDeserializer::deserialize(const std::string &planString,
                                      const std::string &storeRootPath) {
  PhysicalPlanDeserializer deserializer(planString, storeRootPath);
  return deserializer.deserialize();
}

tl::expected<std::shared_ptr<PhysicalPlan>, std::string> PhysicalPlanDeserializer::deserialize() {
  try {
    // parse
    auto jObj = json::parse(planString_);
    if (!jObj.is_object()) {
      return tl::make_unexpected(fmt::format("Cannot parse physical plan JSON '{}", planString_));
    }

    // root
    if (!jObj.contains("root")) {
      return tl::make_unexpected(fmt::format("Root not specified in physical plan JSON '{}'", planString_));
    }
    auto root = jObj["root"].get<std::string>();

    // ops
    if (!jObj.contains("operators")) {
      return tl::make_unexpected(fmt::format("Operators not specified in physical plan JSON '{}'", planString_));
    }
    auto opJArr = jObj["operators"].get<std::vector<json>>();
    std::unordered_map<std::string, std::shared_ptr<PhysicalOp>> opMap;
    for (const auto &opJObj: opJArr) {
      auto expOp = deserializePOp(opJObj);
      if (!expOp.has_value()) {
        return tl::make_unexpected(expOp.error());
      }
      opMap.emplace((*expOp)->name(), *expOp);
    }
    return std::make_shared<PhysicalPlan>(opMap, root);
  } catch (nlohmann::json::parse_error& ex) {
    return tl::make_unexpected(ex.what());
  }
}

tl::expected<std::shared_ptr<PhysicalOp>, std::string>
PhysicalPlanDeserializer::deserializePOp(const ::nlohmann::json &jObj) {
  if (!jObj.contains("type")) {
    return tl::make_unexpected(fmt::format("Type not specified in physical operator JSON '{}'", to_string(jObj)));
  }

  auto type = jObj["type"].get<std::string>();
  if (type == "FPDBStoreFileScanPOp") {
    return deserializeFPDBStoreFileScanPOp(jObj);
  } else if (type == "FilterPOp") {
    return deserializeFilterPOp(jObj);
  } else if (type == "ProjectPOp") {
    return deserializeProjectPOp(jObj);
  } else if (type == "AggregatePOp") {
    return deserializeAggregatePOp(jObj);
  } else if (type == "GroupPOp") {
    return deserializeGroupPOp(jObj);
  } else if (type == "ShufflePOp") {
    return deserializeShufflePOp(jObj);
  } else if (type == "BloomFilterCreatePOp") {
    return deserializeBloomFilterCreatePOp(jObj);
  } else if (type == "BloomFilterUsePOp") {
    return deserializeBloomFilterUsePOp(jObj);
  } else if (type == "HashJoinArrowPOp") {
    return deserializeHashJoinArrowPOp(jObj);
  } else if (type == "CollatePOp") {
    return deserializeCollatePOp(jObj);
  } else {
    return tl::make_unexpected(fmt::format("Unsupported physical operator type at store: '{}'", type));
  }
}

tl::expected<std::shared_ptr<PhysicalOp>, std::string>
PhysicalPlanDeserializer::deserializeFPDBStoreFileScanPOp(const ::nlohmann::json &jObj) {
  // deserialize common
  auto expCommonTuple = deserializePOpCommon(jObj);
  if (!expCommonTuple.has_value()) {
    return tl::make_unexpected(expCommonTuple.error());
  }
  auto commonTuple = *expCommonTuple;
  auto name = std::get<0>(commonTuple);
  auto projectColumnNames = std::get<1>(commonTuple);
  auto producers = std::get<2>(commonTuple);
  auto consumers = std::get<3>(commonTuple);
  auto isSeparated = std::get<4>(commonTuple);
  auto consumerToBloomFilterInfo = std::get<5>(commonTuple);

  // deserialize other
  if (!jObj.contains("bucket")) {
    return tl::make_unexpected(fmt::format("Bucket not specified in FileScanPOp JSON '{}'", to_string(jObj)));
  }
  auto bucket = jObj["bucket"].get<std::string>();

  if (!jObj.contains("object")) {
    return tl::make_unexpected(fmt::format("Object not specified in FileScanPOp JSON '{}'", to_string(jObj)));
  }
  auto object = jObj["object"].get<std::string>();

  if (!jObj.contains("format")) {
    return tl::make_unexpected(fmt::format("Format not specified in FileScanPOp JSON '{}'", to_string(jObj)));
  }
  auto expFormat = FileFormat::fromJson(jObj["format"]);
  if (!expFormat) {
    return tl::make_unexpected(expFormat.error());
  }
  auto format = *expFormat;

  if (!jObj.contains("schema")) {
    return tl::make_unexpected(fmt::format("Schema not specified in FileScanPOp JSON '{}'", to_string(jObj)));
  }
  auto schema = ArrowSerializer::bytes_to_schema(jObj["schema"].get<std::vector<uint8_t>>());

  if (!jObj.contains("fileSize")) {
    return tl::make_unexpected(fmt::format("FileSize not specified in FileScanPOp JSON '{}'", to_string(jObj)));
  }
  auto fileSize = jObj["fileSize"].get<int64_t>();

  std::optional<std::pair<int64_t, int64_t>> byteRange = std::nullopt;
  if (jObj.contains("byteRange")) {
    auto byteRangeJObj = jObj["byteRange"];
    if (!byteRangeJObj.contains("startOffset")) {
      return tl::make_unexpected(fmt::format("StartOffset not specified in byteRange JSON '{}'", to_string(byteRangeJObj)));
    }
    auto startOffset = byteRangeJObj["startOffset"].get<int64_t>();
    if (!byteRangeJObj.contains("finishOffset")) {
      return tl::make_unexpected(fmt::format("FinishOffset not specified in byteRange JSON '{}'", to_string(byteRangeJObj)));
    }
    auto finishOffset = byteRangeJObj["finishOffset"].get<int64_t>();
    byteRange = {startOffset, finishOffset};
  }

  std::shared_ptr<PhysicalOp> storeFileScanPOp = std::make_shared<fpdb_store::FPDBStoreFileScanPOp>(name,
                                                                                                    projectColumnNames,
                                                                                                    0,
                                                                                                    storeRootPath_,
                                                                                                    bucket,
                                                                                                    object,
                                                                                                    format,
                                                                                                    schema,
                                                                                                    fileSize,
                                                                                                    byteRange);
  storeFileScanPOp->setSeparated(isSeparated);
  storeFileScanPOp->setProducers(producers);
  storeFileScanPOp->setConsumers(consumers);
  storeFileScanPOp->setConsumerToBloomFilterInfo(consumerToBloomFilterInfo);

  return storeFileScanPOp;
}

tl::expected<std::shared_ptr<PhysicalOp>, std::string>
PhysicalPlanDeserializer::deserializeFilterPOp(const ::nlohmann::json &jObj) {
  // deserialize common
  auto expCommonTuple = deserializePOpCommon(jObj);
  if (!expCommonTuple.has_value()) {
    return tl::make_unexpected(expCommonTuple.error());
  }
  auto commonTuple = *expCommonTuple;
  auto name = std::get<0>(commonTuple);
  auto projectColumnNames = std::get<1>(commonTuple);
  auto producers = std::get<2>(commonTuple);
  auto consumers = std::get<3>(commonTuple);
  auto isSeparated = std::get<4>(commonTuple);
  auto consumerToBloomFilterInfo = std::get<5>(commonTuple);

  // deserialize other
  if (!jObj.contains("predicate")) {
    return tl::make_unexpected(fmt::format("Predicate not specified in FilterPOp JSON '{}'", to_string(jObj)));
  }
  auto expPredicate = fpdb::expression::gandiva::Expression::fromJson(jObj["predicate"]);
  if (!expPredicate.has_value()) {
    return tl::make_unexpected(expPredicate.error());
  }
  auto predicate = *expPredicate;

  std::shared_ptr<PhysicalOp> filterPOp = std::make_shared<filter::FilterPOp>(name,
                                                                              projectColumnNames,
                                                                              0,
                                                                              predicate);
  filterPOp->setSeparated(isSeparated);
  filterPOp->setProducers(producers);
  filterPOp->setConsumers(consumers);
  filterPOp->setConsumerToBloomFilterInfo(consumerToBloomFilterInfo);

  if (jObj.contains("bitmapWrapper")) {
    auto expBitmapWrapper = FPDBStoreFilterBitmapWrapper::fromJson(jObj["bitmapWrapper"]);
    if (!expBitmapWrapper.has_value()) {
      return tl::make_unexpected(expBitmapWrapper.error());
    }
    std::static_pointer_cast<filter::FilterPOp>(filterPOp)->setBitmapWrapper(*expBitmapWrapper);
  }

  return filterPOp;
}

tl::expected<std::shared_ptr<PhysicalOp>, std::string>
PhysicalPlanDeserializer::deserializeProjectPOp(const ::nlohmann::json &jObj) {
  // deserialize common
  auto expCommonTuple = deserializePOpCommon(jObj);
  if (!expCommonTuple.has_value()) {
    return tl::make_unexpected(expCommonTuple.error());
  }
  auto commonTuple = *expCommonTuple;
  auto name = std::get<0>(commonTuple);
  auto projectColumnNames = std::get<1>(commonTuple);
  auto producers = std::get<2>(commonTuple);
  auto consumers = std::get<3>(commonTuple);
  auto isSeparated = std::get<4>(commonTuple);
  auto consumerToBloomFilterInfo = std::get<5>(commonTuple);

  // deserialize other
  if (!jObj.contains("exprs")) {
    return tl::make_unexpected(fmt::format("Exprs not specified in ProjectPOp JSON '{}'", to_string(jObj)));
  }
  std::vector<std::shared_ptr<fpdb::expression::gandiva::Expression>> exprs;
  auto exprsJArr = jObj["exprs"].get<std::vector<json>>();
  for (const auto &exprJObj: exprsJArr) {
    auto expExpr = fpdb::expression::gandiva::Expression::fromJson(exprJObj);
    if (!expExpr.has_value()) {
      return tl::make_unexpected(expExpr.error());
    }
    exprs.emplace_back(*expExpr);
  }

  if (!jObj.contains("exprNames")) {
    return tl::make_unexpected(fmt::format("ExprNames not specified in ProjectPOp JSON '{}'", to_string(jObj)));
  }
  auto exprNames = jObj["exprNames"].get<std::vector<std::string>>();

  if (!jObj.contains("projectColumnNamePairs")) {
    return tl::make_unexpected(fmt::format("ProjectColumnNamePairs not specified in ProjectPOp JSON '{}'", to_string(jObj)));
  }
  auto projectColumnPairs = jObj["projectColumnNamePairs"].get<std::vector<std::pair<std::string, std::string>>>();

  std::shared_ptr<PhysicalOp> projectPOp = std::make_shared<project::ProjectPOp>(name,
                                                                                 projectColumnNames,
                                                                                 0,
                                                                                 exprs,
                                                                                 exprNames,
                                                                                 projectColumnPairs);
  projectPOp->setSeparated(isSeparated);
  projectPOp->setProducers(producers);
  projectPOp->setConsumers(consumers);
  projectPOp->setConsumerToBloomFilterInfo(consumerToBloomFilterInfo);

  return projectPOp;
}

tl::expected<std::shared_ptr<PhysicalOp>, std::string>
PhysicalPlanDeserializer::deserializeAggregatePOp(const ::nlohmann::json &jObj) {
  // deserialize common
  auto expCommonTuple = deserializePOpCommon(jObj);
  if (!expCommonTuple.has_value()) {
    return tl::make_unexpected(expCommonTuple.error());
  }
  auto commonTuple = *expCommonTuple;
  auto name = std::get<0>(commonTuple);
  auto projectColumnNames = std::get<1>(commonTuple);
  auto producers = std::get<2>(commonTuple);
  auto consumers = std::get<3>(commonTuple);
  auto isSeparated = std::get<4>(commonTuple);
  auto consumerToBloomFilterInfo = std::get<5>(commonTuple);

  // deserialize other
  if (!jObj.contains("functions")) {
    return tl::make_unexpected(fmt::format("Aggregate functions not specified in AggregatePOp JSON '{}'", to_string(jObj)));
  }
  std::vector<std::shared_ptr<aggregate::AggregateFunction>> functions;
  auto functionsJArr = jObj["functions"].get<std::vector<json>>();
  for (const auto &functionJObj: functionsJArr) {
    auto expFunction = aggregate::AggregateFunction::fromJson(functionJObj);
    if (!expFunction.has_value()) {
      return tl::make_unexpected(expFunction.error());
    }
    functions.emplace_back(*expFunction);
  }

  std::shared_ptr<PhysicalOp> aggregatePOp = std::make_shared<aggregate::AggregatePOp>(name,
                                                                                       projectColumnNames,
                                                                                       0,
                                                                                       functions);
  aggregatePOp->setSeparated(isSeparated);
  aggregatePOp->setProducers(producers);
  aggregatePOp->setConsumers(consumers);
  aggregatePOp->setConsumerToBloomFilterInfo(consumerToBloomFilterInfo);

  return aggregatePOp;
}

tl::expected<std::shared_ptr<PhysicalOp>, std::string>
PhysicalPlanDeserializer::deserializeGroupPOp(const ::nlohmann::json &jObj) {
  // deserialize common
  auto expCommonTuple = deserializePOpCommon(jObj);
  if (!expCommonTuple.has_value()) {
    return tl::make_unexpected(expCommonTuple.error());
  }
  auto commonTuple = *expCommonTuple;
  auto name = std::get<0>(commonTuple);
  auto projectColumnNames = std::get<1>(commonTuple);
  auto producers = std::get<2>(commonTuple);
  auto consumers = std::get<3>(commonTuple);
  auto isSeparated = std::get<4>(commonTuple);
  auto consumerToBloomFilterInfo = std::get<5>(commonTuple);

  // deserialize other
  if (!jObj.contains("kernel")) {
    return tl::make_unexpected(fmt::format("Kernel not specified in GroupPOp JSON '{}'", to_string(jObj)));
  }
  auto expKernel = group::GroupAbstractKernel::fromJson(jObj["kernel"]);
  if (!expKernel.has_value()) {
    return tl::make_unexpected(expKernel.error());
  }

  std::shared_ptr<PhysicalOp> groupPOp = std::make_shared<group::GroupPOp>(name,
                                                                           projectColumnNames,
                                                                           0,
                                                                           *expKernel);
  groupPOp->setSeparated(isSeparated);
  groupPOp->setProducers(producers);
  groupPOp->setConsumers(consumers);
  groupPOp->setConsumerToBloomFilterInfo(consumerToBloomFilterInfo);

  return groupPOp;
}

tl::expected<std::shared_ptr<PhysicalOp>, std::string>
PhysicalPlanDeserializer::deserializeShufflePOp(const ::nlohmann::json &jObj) {
  // deserialize common
  auto expCommonTuple = deserializePOpCommon(jObj);
  if (!expCommonTuple.has_value()) {
    return tl::make_unexpected(expCommonTuple.error());
  }
  auto commonTuple = *expCommonTuple;
  auto name = std::get<0>(commonTuple);
  auto projectColumnNames = std::get<1>(commonTuple);
  auto producers = std::get<2>(commonTuple);
  auto consumers = std::get<3>(commonTuple);
  auto isSeparated = std::get<4>(commonTuple);
  auto consumerToBloomFilterInfo = std::get<5>(commonTuple);

  // deserialize other
  if (!jObj.contains("shuffleColumnNames")) {
    return tl::make_unexpected(fmt::format("ShuffleColumnNames not specified in ShufflePOp JSON '{}'", to_string(jObj)));
  }
  auto shuffleColumnNames = jObj["shuffleColumnNames"].get<std::vector<std::string>>();

  if (!jObj.contains("consumerVec")) {
    return tl::make_unexpected(fmt::format("ConsumerVec not specified in ShufflePOp JSON '{}'", to_string(jObj)));
  }
  auto consumerVec = jObj["consumerVec"].get<std::vector<std::string>>();

  std::shared_ptr<PhysicalOp> shufflePOp = std::make_shared<shuffle::ShufflePOp>(name,
                                                                                 projectColumnNames,
                                                                                 0,
                                                                                 shuffleColumnNames,
                                                                                 consumerVec);
  shufflePOp->setSeparated(isSeparated);
  shufflePOp->setProducers(producers);
  shufflePOp->setConsumers(consumers);
  shufflePOp->setConsumerToBloomFilterInfo(consumerToBloomFilterInfo);

  return shufflePOp;
}

tl::expected<std::shared_ptr<PhysicalOp>, std::string>
PhysicalPlanDeserializer::deserializeBloomFilterCreatePOp(const ::nlohmann::json &jObj) {
  // deserialize common
  auto expCommonTuple = deserializePOpCommon(jObj);
  if (!expCommonTuple.has_value()) {
    return tl::make_unexpected(expCommonTuple.error());
  }
  auto commonTuple = *expCommonTuple;
  auto name = std::get<0>(commonTuple);
  auto projectColumnNames = std::get<1>(commonTuple);
  auto producers = std::get<2>(commonTuple);
  auto consumers = std::get<3>(commonTuple);
  auto isSeparated = std::get<4>(commonTuple);
  auto consumerToBloomFilterInfo = std::get<5>(commonTuple);

  // deserialize other
  if (!jObj.contains("bloomFilterColumnNames")) {
    return tl::make_unexpected(fmt::format("BloomFilterColumnNames not specified in BloomFilterCreatePOp JSON '{}'", to_string(jObj)));
  }
  auto bloomFilterColumnNames = jObj["bloomFilterColumnNames"].get<std::vector<std::string>>();

  if (!jObj.contains("desiredFalsePositiveRate")) {
    return tl::make_unexpected(fmt::format("DesiredFalsePositiveRate not specified in BloomFilterCreatePOp JSON '{}'", to_string(jObj)));
  }
  auto desiredFalsePositiveRate = jObj["desiredFalsePositiveRate"].get<double>();

  if (!jObj.contains("bloomFilterUsePOps")) {
    return tl::make_unexpected(fmt::format("BloomFilterUsePOps not specified in BloomFilterCreatePOp JSON '{}'", to_string(jObj)));
  }
  auto bloomFilterUsePOps = jObj["bloomFilterUsePOps"].get<std::set<std::string>>();

  if (!jObj.contains("passTupleSetConsumers")) {
    return tl::make_unexpected(fmt::format("PassTupleSetConsumers not specified in BloomFilterCreatePOp JSON '{}'", to_string(jObj)));
  }
  auto passTupleSetConsumers = jObj["passTupleSetConsumers"].get<std::set<std::string>>();

  auto bloomFilterCreatePOp = std::make_shared<bloomfilter::BloomFilterCreatePOp>(name,
                                                                                  projectColumnNames,
                                                                                  0,
                                                                                  bloomFilterColumnNames,
                                                                                  desiredFalsePositiveRate);
  bloomFilterCreatePOp->setSeparated(isSeparated);
  bloomFilterCreatePOp->setProducers(producers);
  bloomFilterCreatePOp->setConsumers(consumers);
  bloomFilterCreatePOp->setConsumerToBloomFilterInfo(consumerToBloomFilterInfo);
  bloomFilterCreatePOp->setBloomFilterUsePOps(bloomFilterUsePOps);
  bloomFilterCreatePOp->setPassTupleSetConsumers(passTupleSetConsumers);

  return bloomFilterCreatePOp;
}

tl::expected<std::shared_ptr<PhysicalOp>, std::string>
PhysicalPlanDeserializer::deserializeBloomFilterUsePOp(const ::nlohmann::json &jObj) {
  // deserialize common
  auto expCommonTuple = deserializePOpCommon(jObj);
  if (!expCommonTuple.has_value()) {
    return tl::make_unexpected(expCommonTuple.error());
  }
  auto commonTuple = *expCommonTuple;
  auto name = std::get<0>(commonTuple);
  auto projectColumnNames = std::get<1>(commonTuple);
  auto producers = std::get<2>(commonTuple);
  auto consumers = std::get<3>(commonTuple);
  auto isSeparated = std::get<4>(commonTuple);
  auto consumerToBloomFilterInfo = std::get<5>(commonTuple);

  // deserialize other
  if (!jObj.contains("bloomFilterColumnNames")) {
    return tl::make_unexpected(fmt::format("BloomFilterColumnNames not specified in BloomFilterUsePOp JSON '{}'", to_string(jObj)));
  }
  auto bloomFilterColumnNames = jObj["bloomFilterColumnNames"].get<std::vector<std::string>>();

  auto bloomFilterUsePOp = std::make_shared<bloomfilter::BloomFilterUsePOp>(name,
                                                                            projectColumnNames,
                                                                            0,
                                                                            bloomFilterColumnNames);
  bloomFilterUsePOp->setSeparated(isSeparated);
  bloomFilterUsePOp->setProducers(producers);
  bloomFilterUsePOp->setConsumers(consumers);
  bloomFilterUsePOp->setConsumerToBloomFilterInfo(consumerToBloomFilterInfo);

  return bloomFilterUsePOp;
}

tl::expected<std::shared_ptr<PhysicalOp>, std::string>
PhysicalPlanDeserializer::deserializeHashJoinArrowPOp(const ::nlohmann::json &jObj) {
  // deserialize common
  auto expCommonTuple = deserializePOpCommon(jObj);
  if (!expCommonTuple.has_value()) {
    return tl::make_unexpected(expCommonTuple.error());
  }
  auto commonTuple = *expCommonTuple;
  auto name = std::get<0>(commonTuple);
  auto projectColumnNames = std::get<1>(commonTuple);
  auto producers = std::get<2>(commonTuple);
  auto consumers = std::get<3>(commonTuple);
  auto isSeparated = std::get<4>(commonTuple);
  auto consumerToBloomFilterInfo = std::get<5>(commonTuple);

  // deserialize other
  if (!jObj.contains("pred")) {
    return tl::make_unexpected(fmt::format("Pred not specified in HashJoinArrowPOp JSON '{}'", to_string(jObj)));
  }
  auto expPred = join::HashJoinPredicate::fromJson(jObj["pred"]);
  if (!expPred.has_value()) {
    return tl::make_unexpected(expPred.error());
  }

  if (!jObj.contains("joinType")) {
    return tl::make_unexpected(fmt::format("JoinType not specified in HashJoinArrowPOp JSON '{}'", to_string(jObj)));
  }
  auto joinType = jObj["joinType"].get<JoinType>();

  if (!jObj.contains("buildProducers")) {
    return tl::make_unexpected(fmt::format("BuildProducers not specified in HashJoinArrowPOp JSON '{}'", to_string(jObj)));
  }
  auto buildProducers = jObj["buildProducers"].get<std::set<std::string>>();

  if (!jObj.contains("probeProducers")) {
    return tl::make_unexpected(fmt::format("ProbeProducers not specified in HashJoinArrowPOp JSON '{}'", to_string(jObj)));
  }
  auto probeProducers = jObj["probeProducers"].get<std::set<std::string>>();

  auto hashJoinArrowPOp = std::make_shared<join::HashJoinArrowPOp>(name,
                                                                   projectColumnNames,
                                                                   0,
                                                                   *expPred,
                                                                   joinType);
  hashJoinArrowPOp->setSeparated(isSeparated);
  hashJoinArrowPOp->setProducers(producers);
  hashJoinArrowPOp->setConsumers(consumers);
  hashJoinArrowPOp->setConsumerToBloomFilterInfo(consumerToBloomFilterInfo);
  hashJoinArrowPOp->setBuildProducers(buildProducers);
  hashJoinArrowPOp->setProbeProducers(probeProducers);

  return hashJoinArrowPOp;
}

tl::expected<std::shared_ptr<PhysicalOp>, std::string>
PhysicalPlanDeserializer::deserializeCollatePOp(const ::nlohmann::json &jObj) {
  // deserialize self
  auto expCommonTuple = deserializePOpCommon(jObj);
  if (!expCommonTuple.has_value()) {
    return tl::make_unexpected(expCommonTuple.error());
  }
  auto commonTuple = *expCommonTuple;
  auto name = std::get<0>(commonTuple);
  auto projectColumnNames = std::get<1>(commonTuple);
  auto producers = std::get<2>(commonTuple);
  auto consumers = std::get<3>(commonTuple);
  auto isSeparated = std::get<4>(commonTuple);
  auto consumerToBloomFilterInfo = std::get<5>(commonTuple);

  // deserialize other
  if (!jObj.contains("forward")) {
    return tl::make_unexpected(fmt::format("Forward not specified in CollatePOp JSON '{}'", to_string(jObj)));
  }
  auto forward = jObj["forward"].get<bool>();

  if (!jObj.contains("forwardConsumers")) {
    return tl::make_unexpected(fmt::format("ForwardConsumers not specified in CollatePOp JSON '{}'", to_string(jObj)));
  }
  auto forwardConsumers = jObj["forwardConsumers"].get<std::unordered_map<std::string, std::string>>();

  if (!jObj.contains("endConsumers")) {
    return tl::make_unexpected(fmt::format("EndConsumers not specified in CollatePOp JSON '{}'", to_string(jObj)));
  }
  auto endConsumers = jObj["endConsumers"].get<std::vector<std::string>>();

  auto collatePOp = std::make_shared<fpdb::executor::physical::collate::CollatePOp>(name,
                                                                                    projectColumnNames,
                                                                                    0);
  collatePOp->setSeparated(isSeparated);
  collatePOp->setProducers(producers);
  collatePOp->setConsumers(consumers);
  collatePOp->setConsumerToBloomFilterInfo(consumerToBloomFilterInfo);
  collatePOp->setForward(forward);
  collatePOp->setForwardConsumers(forwardConsumers);
  collatePOp->setEndConsumers(endConsumers);

  return collatePOp;
}

tl::expected<std::tuple<std::string,
                        std::vector<std::string>,
                        std::set<std::string>,
                        std::set<std::string>,
                        bool,
                        std::unordered_map<std::string, std::shared_ptr<fpdb_store::FPDBStoreBloomFilterUseInfo>>>,
             std::string>
PhysicalPlanDeserializer::deserializePOpCommon(const ::nlohmann::json &jObj) {
  if (!jObj.contains("name")) {
    return tl::make_unexpected(fmt::format("Name not specified in physical operator JSON '{}'", to_string(jObj)));
  }
  auto name = jObj["name"].get<std::string>();

  if (!jObj.contains("projectColumnNames")) {
    return tl::make_unexpected(fmt::format("ProjectColumnNames not specified in physical operator JSON '{}'", to_string(jObj)));
  }
  auto projectColumnNames = jObj["projectColumnNames"].get<std::vector<std::string>>();

  if (!jObj.contains("producers")) {
    return tl::make_unexpected(fmt::format("Producers not specified in physical operator JSON '{}'", to_string(jObj)));
  }
  auto producers = jObj["producers"].get<std::set<std::string>>();

  if (!jObj.contains("consumers")) {
    return tl::make_unexpected(fmt::format("Consumers not specified in physical operator JSON '{}'", to_string(jObj)));
  }
  auto consumers = jObj["consumers"].get<std::set<std::string>>();

  if (!jObj.contains("isSeparated")) {
    return tl::make_unexpected(fmt::format("IsSeparated not specified in physical operator JSON '{}'", to_string(jObj)));
  }
  auto isSeparated = jObj["isSeparated"].get<bool>();

  if (!jObj.contains("consumerToBloomFilterInfo")) {
    return tl::make_unexpected(
            fmt::format("ConsumerToBloomFilterInfo not specified in physical operator JSON '{}'", to_string(jObj)));
  }
  std::unordered_map<std::string, std::shared_ptr<fpdb_store::FPDBStoreBloomFilterUseInfo>> consumerToBloomFilterInfo;
  const auto &consumerToBloomFilterInfoJMap =
          jObj["consumerToBloomFilterInfo"].get<std::unordered_map<std::string, json>>();
  for (const auto &consumerToBloomFilterInfoJIt: consumerToBloomFilterInfoJMap) {
    auto expBloomFilterInfo = fpdb_store::FPDBStoreBloomFilterUseInfo::fromJson(consumerToBloomFilterInfoJIt.second);
    if (!expBloomFilterInfo.has_value()) {
      return tl::make_unexpected(expBloomFilterInfo.error());
    }
    consumerToBloomFilterInfo.emplace(consumerToBloomFilterInfoJIt.first, *expBloomFilterInfo);
  }

  return std::tuple<std::string, std::vector<std::string>, std::set<std::string>, std::set<std::string>, bool,
                    std::unordered_map<std::string, std::shared_ptr<fpdb_store::FPDBStoreBloomFilterUseInfo>>>
                    {name, projectColumnNames, producers, consumers, isSeparated, consumerToBloomFilterInfo};
}

}
