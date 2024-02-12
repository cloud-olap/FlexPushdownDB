//
// Created by Yifei Yang on 9/14/22.
//

#ifndef FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_FPDB_STORE_FPDBSTOREBLOOMFILTERINFO_H
#define FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_FPDB_STORE_FPDBSTOREBLOOMFILTERINFO_H

#include <fpdb/executor/physical/bloomfilter/BloomFilterBase.h>
#include <fpdb/caf/CAFUtil.h>
#include <string>

namespace fpdb::executor::physical::fpdb_store {

/**
 * Information needed to send bloom filter to fpdb-store
 */
struct FPDBStoreBloomFilterCreateInfo {
  // used to connect to store flight, here denoted as <host, num copies>
  // num copies -> num copies to consume (each bloom filter is consumed by multiple shuffle ops at storage side)
  std::unordered_map<std::string, int> hosts_;
  int port_;

  // caf inspect
  template <class Inspector>
  friend bool inspect(Inspector& f, FPDBStoreBloomFilterCreateInfo& info) {
    return f.object(info).fields(f.field("hosts", info.hosts_),
                                 f.field("port", info.port_));
  }
};

/**
 * Information needed to use bloom filter at fpdb-store
 */
struct FPDBStoreBloomFilterUseInfo {
  FPDBStoreBloomFilterUseInfo(const std::string bloomFilterCreatePOp, const std::vector<std::string> &columnNames,
          const std::optional<std::shared_ptr<bloomfilter::BloomFilterBase>>& bloomFilter = std::nullopt);
  FPDBStoreBloomFilterUseInfo() = default;
  FPDBStoreBloomFilterUseInfo(const FPDBStoreBloomFilterUseInfo&) = default;
  FPDBStoreBloomFilterUseInfo& operator=(const FPDBStoreBloomFilterUseInfo&) = default;

  // serialization
  ::nlohmann::json toJson() const;
  static tl::expected<std::shared_ptr<FPDBStoreBloomFilterUseInfo>, std::string> fromJson(const ::nlohmann::json &jObj);

  // op name to create this bloom filter at compute side
  std::string bloomFilterCreatePOp_;

  // columns to apply bloom filter
  std::vector<std::string> columnNames_;

  // bloom filter (only set at storage side)
  std::optional<std::shared_ptr<bloomfilter::BloomFilterBase>> bloomFilter_;

  // caf inspect
  template <class Inspector>
  friend bool inspect(Inspector& f, FPDBStoreBloomFilterUseInfo& info) {
    return f.object(info).fields(f.field("bloomFilterCreatePOp", info.bloomFilterCreatePOp_),
                                 f.field("columnNames", info.columnNames_));
  }
};

}

using FPDBStoreBloomFilterUseInfoPtr = std::shared_ptr<fpdb::executor::physical::fpdb_store::FPDBStoreBloomFilterUseInfo>;

CAF_BEGIN_TYPE_ID_BLOCK(FPDBStoreBloomFilterUseInfo, fpdb::caf::CAFUtil::FPDBStoreBloomFilterUseInfo_first_custom_type_id)
CAF_ADD_TYPE_ID(FPDBStoreBloomFilterUseInfo, (fpdb::executor::physical::fpdb_store::FPDBStoreBloomFilterUseInfo))
CAF_END_TYPE_ID_BLOCK(FPDBStoreBloomFilterUseInfo)

namespace caf {
  template <>
  struct inspector_access<FPDBStoreBloomFilterUseInfoPtr> : variant_inspector_access<FPDBStoreBloomFilterUseInfoPtr> {
    // nop
  };
} // namespace caf

#endif //FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_FPDB_STORE_FPDBSTOREBLOOMFILTERINFO_H
