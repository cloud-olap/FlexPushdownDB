//
// Created by Yifei Yang on 2/21/22.
//

#ifndef FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_SERIALIZATION_PHYSICALPLANSERIALIZER_H
#define FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_SERIALIZATION_PHYSICALPLANSERIALIZER_H

#include <fpdb/executor/physical/PhysicalPlan.h>
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
#include <tl/expected.hpp>
#include <nlohmann/json.hpp>

namespace fpdb::executor::physical {

class PhysicalPlanSerializer {

public:
  static tl::expected<std::string, std::string> serialize(const std::shared_ptr<PhysicalPlan> &physicalPlan,
                                                          bool pretty);

private:
  PhysicalPlanSerializer(const std::shared_ptr<PhysicalPlan> &physicalPlan,
                         bool pretty);

  /**
   * Impl of serialization
   * @return
   */
  tl::expected<std::string, std::string> serialize();

  tl::expected<::nlohmann::json, std::string> serializePOp(const std::shared_ptr<PhysicalOp> &op);

  tl::expected<::nlohmann::json, std::string>
  serializeFPDBStoreFileScanPOp(const std::shared_ptr<fpdb_store::FPDBStoreFileScanPOp> &storeFileScanPOp);

  tl::expected<::nlohmann::json, std::string>
  serializeFilterPOp(const std::shared_ptr<filter::FilterPOp> &filterPOp);

  tl::expected<::nlohmann::json, std::string>
  serializeProjectPOp(const std::shared_ptr<project::ProjectPOp> &projectPOp);

  tl::expected<::nlohmann::json, std::string>
  serializeAggregatePOp(const std::shared_ptr<aggregate::AggregatePOp> &aggregatePOp);

  tl::expected<::nlohmann::json, std::string>
  serializeGroupPOp(const std::shared_ptr<group::GroupPOp> &groupPOp);

  tl::expected<::nlohmann::json, std::string>
  serializeShufflePOp(const std::shared_ptr<shuffle::ShufflePOp> &shufflePOp);

  tl::expected<::nlohmann::json, std::string>
  serializeBloomFilterCreatePOp(const std::shared_ptr<bloomfilter::BloomFilterCreatePOp> &bloomFilterCreatePOp);

  tl::expected<::nlohmann::json, std::string>
  serializeBloomFilterUsePOp(const std::shared_ptr<bloomfilter::BloomFilterUsePOp> &bloomFilterUsePOp);

  tl::expected<::nlohmann::json, std::string>
  serializeHashJoinArrowPOp(const std::shared_ptr<join::HashJoinArrowPOp> &hashJoinArrowPOp);

  tl::expected<::nlohmann::json, std::string>
  serializeCollatePOp(const std::shared_ptr<collate::CollatePOp> &collatePOp);

  ::nlohmann::json serializePOpCommon(const std::shared_ptr<PhysicalOp> &op);

  std::shared_ptr<PhysicalPlan> physicalPlan_;
  bool pretty_;

};

}


#endif //FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_SERIALIZATION_PHYSICALPLANSERIALIZER_H
