//
// Created by Yifei Yang on 2/21/22.
//

#include <fpdb/executor/physical/serialization/PhysicalPlanSerializer.h>
#include <fpdb/executor/physical/bloomfilter/BloomFilterCreateKernel.h>
#include <fpdb/tuple/serialization/ArrowSerializer.h>

using namespace fpdb::tuple;
using json = nlohmann::json;

namespace fpdb::executor::physical {

PhysicalPlanSerializer::PhysicalPlanSerializer(const std::shared_ptr<PhysicalPlan> &physicalPlan,
                                               bool pretty):
  physicalPlan_(physicalPlan),
  pretty_(pretty){}

tl::expected<std::string, std::string>
PhysicalPlanSerializer::serialize(const std::shared_ptr<PhysicalPlan> &physicalPlan,
                                  bool pretty) {
  PhysicalPlanSerializer serializer(physicalPlan, pretty);
  return serializer.serialize();
}

tl::expected<std::string, std::string> PhysicalPlanSerializer::serialize() {
  // root op
  json jObj;
  jObj.emplace("root", physicalPlan_->getRootPOpName());

  // ops
  vector<json> opJArr;
  for (const auto &opIt: physicalPlan_->getPhysicalOps()) {
    auto expOpJObj = serializePOp(opIt.second);
    if (!expOpJObj.has_value()) {
      return tl::make_unexpected(expOpJObj.error());
    }
    opJArr.emplace_back(*expOpJObj);
  }
  jObj.emplace("operators", opJArr);
  return jObj.dump(pretty_ ? 2 : -1);
}

tl::expected<json, std::string> PhysicalPlanSerializer::serializePOp(const std::shared_ptr<PhysicalOp> &op) {
  switch (op->getType()) {
    case POpType::FPDB_STORE_FILE_SCAN:
      return serializeFPDBStoreFileScanPOp(std::static_pointer_cast<fpdb_store::FPDBStoreFileScanPOp>(op));
    case POpType::FILTER:
      return serializeFilterPOp(std::static_pointer_cast<filter::FilterPOp>(op));
    case POpType::PROJECT:
      return serializeProjectPOp(std::static_pointer_cast<project::ProjectPOp>(op));
    case POpType::AGGREGATE:
      return serializeAggregatePOp(std::static_pointer_cast<aggregate::AggregatePOp>(op));
    case POpType::GROUP:
      return serializeGroupPOp(std::static_pointer_cast<group::GroupPOp>(op));
    case POpType::SHUFFLE:
      return serializeShufflePOp(std::static_pointer_cast<shuffle::ShufflePOp>(op));
    case POpType::BLOOM_FILTER_CREATE:
      return serializeBloomFilterCreatePOp(std::static_pointer_cast<bloomfilter::BloomFilterCreatePOp>(op));
    case POpType::BLOOM_FILTER_USE:
      return serializeBloomFilterUsePOp(std::static_pointer_cast<bloomfilter::BloomFilterUsePOp>(op));
    case POpType::HASH_JOIN_ARROW:
      return serializeHashJoinArrowPOp(std::static_pointer_cast<join::HashJoinArrowPOp>(op));
    case POpType::COLLATE:
      return serializeCollatePOp(std::static_pointer_cast<collate::CollatePOp>(op));
    default:
      return tl::make_unexpected(fmt::format("Unsupported physical operator type at store: '{}'",
                                             op->getTypeString()));
  }
}

tl::expected<::nlohmann::json, std::string> PhysicalPlanSerializer::serializeFPDBStoreFileScanPOp(
        const std::shared_ptr<fpdb_store::FPDBStoreFileScanPOp> &storeFileScanPOp) {
  auto jObj = serializePOpCommon(storeFileScanPOp);

  auto kernel = storeFileScanPOp->getKernel();
  jObj.emplace("bucket", storeFileScanPOp->getBucket());
  jObj.emplace("object", storeFileScanPOp->getObject());
  jObj.emplace("format", kernel->getFormat()->toJson());
  jObj.emplace("schema", ArrowSerializer::schema_to_bytes(kernel->getSchema()));
  jObj.emplace("fileSize", kernel->getFileSize());
  auto optByteRange = kernel->getByteRange();
  if (optByteRange.has_value()) {
    json byteRangeJObj;
    byteRangeJObj.emplace("startOffset", optByteRange->first);
    byteRangeJObj.emplace("finishOffset", optByteRange->second);
    jObj.emplace("byteRange", byteRangeJObj);
  }

  return jObj;
}

tl::expected<::nlohmann::json, std::string>
PhysicalPlanSerializer::serializeFilterPOp(const std::shared_ptr<filter::FilterPOp> &filterPOp) {
  auto jObj = serializePOpCommon(filterPOp);

  jObj.emplace("predicate", filterPOp->getPredicate()->toJson());
  auto bitmapWrapper = filterPOp->getBitmapWrapper();
  if (bitmapWrapper.has_value()) {
    jObj.emplace("bitmapWrapper", bitmapWrapper->toJson());
  }

  return jObj;
}

tl::expected<::nlohmann::json, std::string>
PhysicalPlanSerializer::serializeProjectPOp(const std::shared_ptr<project::ProjectPOp> &projectPOp) {
  auto jObj = serializePOpCommon(projectPOp);

  vector<json> exprsJArr;
  for (const auto &expr: projectPOp->getExprs()) {
    exprsJArr.emplace_back(expr->toJson());
  }
  jObj.emplace("exprs", exprsJArr);
  jObj.emplace("exprNames", projectPOp->getExprNames());
  jObj.emplace("projectColumnNamePairs", projectPOp->getProjectColumnNamePairs());;

  return jObj;
}

tl::expected<::nlohmann::json, std::string>
PhysicalPlanSerializer::serializeAggregatePOp(const std::shared_ptr<aggregate::AggregatePOp> &aggregatePOp) {
  auto jObj = serializePOpCommon(aggregatePOp);

  vector<json> functionsJArr;
  for (const auto &function: aggregatePOp->getFunctions()) {
    functionsJArr.emplace_back(function->toJson());
  }
  jObj.emplace("functions", functionsJArr);

  return jObj;
}

tl::expected<::nlohmann::json, std::string>
PhysicalPlanSerializer::serializeGroupPOp(const std::shared_ptr<group::GroupPOp> &groupPOp) {
  auto jObj = serializePOpCommon(groupPOp);

  jObj.emplace("kernel", groupPOp->getKernel()->toJson());

  return jObj;
}

tl::expected<::nlohmann::json, std::string>
PhysicalPlanSerializer::serializeShufflePOp(const std::shared_ptr<shuffle::ShufflePOp> &shufflePOp) {
  auto jObj = serializePOpCommon(shufflePOp);

  jObj.emplace("shuffleColumnNames", shufflePOp->getShuffleColumnNames());
  jObj.emplace("consumerVec", shufflePOp->getConsumerVec());

  return jObj;
}

tl::expected<::nlohmann::json, std::string> PhysicalPlanSerializer::serializeBloomFilterCreatePOp(
        const std::shared_ptr<bloomfilter::BloomFilterCreatePOp> &bloomFilterCreatePOp) {
  auto jObj = serializePOpCommon(bloomFilterCreatePOp);

  const auto &kernel = bloomFilterCreatePOp->getKernel();
  jObj.emplace("bloomFilterColumnNames", kernel->getColumnNames());
  jObj.emplace("desiredFalsePositiveRate", kernel->getType() == BloomFilterCreateKernelType::BLOOM_FILTER_KERNEL ?
        std::static_pointer_cast<BloomFilterCreateKernel>(kernel)->getDesiredFalsePositiveRate() :
        BloomFilter::DefaultDesiredFalsePositiveRate);
  jObj.emplace("bloomFilterUsePOps", bloomFilterCreatePOp->getBloomFilterUsePOps());
  jObj.emplace("passTupleSetConsumers", bloomFilterCreatePOp->getPassTupleSetConsumers());

  return jObj;
}

tl::expected<::nlohmann::json, std::string> PhysicalPlanSerializer::serializeBloomFilterUsePOp(
        const std::shared_ptr<bloomfilter::BloomFilterUsePOp> &bloomFilterUsePOp) {
  auto jObj = serializePOpCommon(bloomFilterUsePOp);

  jObj.emplace("bloomFilterColumnNames", bloomFilterUsePOp->getBloomFilterColumnNames());

  return jObj;
}

tl::expected<::nlohmann::json, std::string>
PhysicalPlanSerializer::serializeHashJoinArrowPOp(const std::shared_ptr<join::HashJoinArrowPOp> &hashJoinArrowPOp) {
  auto jObj = serializePOpCommon(hashJoinArrowPOp);

  jObj.emplace("pred", hashJoinArrowPOp->getKernel().getPred().toJson());
  jObj.emplace("joinType", hashJoinArrowPOp->getKernel().getJoinType());
  jObj.emplace("buildProducers", hashJoinArrowPOp->getBuildProducers());
  jObj.emplace("probeProducers", hashJoinArrowPOp->getProbeProducers());

  return jObj;
}

tl::expected<::nlohmann::json, std::string>
PhysicalPlanSerializer::serializeCollatePOp(const std::shared_ptr<collate::CollatePOp> &collatePOp) {
  auto jObj = serializePOpCommon(collatePOp);

  jObj.emplace("forward", collatePOp->isForward());
  jObj.emplace("forwardConsumers", collatePOp->getForwardConsumers());
  jObj.emplace("endConsumers", collatePOp->getEndConsumers());

  return jObj;
}

::nlohmann::json PhysicalPlanSerializer::serializePOpCommon(const std::shared_ptr<PhysicalOp> &op) {
  json jObj;
  jObj.emplace("type", op->getTypeString());
  jObj.emplace("name", op->name());
  jObj.emplace("projectColumnNames", op->getProjectColumnNames());
  jObj.emplace("producers", op->producers());
  jObj.emplace("consumers", op->consumers());
  jObj.emplace("isSeparated", op->isSeparated());

  std::unordered_map<std::string, json> consumerToBloomFilterInfoJMap;
  for (const auto &consumerToBloomFilterInfoIt: op->getConsumerToBloomFilterInfo()) {
    consumerToBloomFilterInfoJMap.emplace(consumerToBloomFilterInfoIt.first,
                                          consumerToBloomFilterInfoIt.second->toJson());
  }
  jObj.emplace("consumerToBloomFilterInfo", consumerToBloomFilterInfoJMap);

  return jObj;
}

}
