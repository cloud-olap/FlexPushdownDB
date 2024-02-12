//
// Created by Yifei Yang on 2/21/22.
//

#ifndef FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_SERIALIZATION_PHYSICALPLANDESERIALIZER_H
#define FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_SERIALIZATION_PHYSICALPLANDESERIALIZER_H

#include <fpdb/executor/physical/PhysicalPlan.h>
#include <tl/expected.hpp>

namespace fpdb::executor::physical {

class PhysicalPlanDeserializer {

public:
  static tl::expected<std::shared_ptr<PhysicalPlan>, std::string> deserialize(const std::string &planString,
                                                                              const std::string &storeRootPath);

private:
  PhysicalPlanDeserializer(const std::string &planString,
                           const std::string &storeRootPath);

  /**
   * Impl of deserialization
   * @return
   */
  tl::expected<std::shared_ptr<PhysicalPlan>, std::string> deserialize();

  tl::expected<std::shared_ptr<PhysicalOp>, std::string> deserializePOp(const ::nlohmann::json &jObj);

  tl::expected<std::shared_ptr<PhysicalOp>, std::string> deserializeFPDBStoreFileScanPOp(const ::nlohmann::json &jObj);

  tl::expected<std::shared_ptr<PhysicalOp>, std::string> deserializeFilterPOp(const ::nlohmann::json &jObj);

  tl::expected<std::shared_ptr<PhysicalOp>, std::string> deserializeProjectPOp(const ::nlohmann::json &jObj);

  tl::expected<std::shared_ptr<PhysicalOp>, std::string> deserializeAggregatePOp(const ::nlohmann::json &jObj);

  tl::expected<std::shared_ptr<PhysicalOp>, std::string> deserializeGroupPOp(const ::nlohmann::json &jObj);

  tl::expected<std::shared_ptr<PhysicalOp>, std::string> deserializeShufflePOp(const ::nlohmann::json &jObj);

  tl::expected<std::shared_ptr<PhysicalOp>, std::string> deserializeBloomFilterCreatePOp(const ::nlohmann::json &jObj);

  tl::expected<std::shared_ptr<PhysicalOp>, std::string> deserializeBloomFilterUsePOp(const ::nlohmann::json &jObj);

  tl::expected<std::shared_ptr<PhysicalOp>, std::string> deserializeHashJoinArrowPOp(const ::nlohmann::json &jObj);

  tl::expected<std::shared_ptr<PhysicalOp>, std::string> deserializeCollatePOp(const ::nlohmann::json &jObj);

  tl::expected<std::tuple<std::string,
                          std::vector<std::string>,
                          std::set<std::string>,
                          std::set<std::string>,
                          bool,
                          std::unordered_map<std::string, std::shared_ptr<fpdb_store::FPDBStoreBloomFilterUseInfo>>>,
               std::string>
  deserializePOpCommon(const ::nlohmann::json &jObj);

  std::string planString_;
  std::string storeRootPath_;

};

}


#endif //FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_SERIALIZATION_PHYSICALPLANDESERIALIZER_H
