//
// Created by Yifei Yang on 11/20/21.
//

#ifndef FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_PHYSICALPLAN_H
#define FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_PHYSICALPLAN_H

#include <fpdb/executor/physical/PhysicalOp.h>
#include <fpdb/caf/CAFUtil.h>
#include <tl/expected.hpp>
#include <utility>

using namespace std;

namespace fpdb::executor::physical {

class PhysicalPlan {
public:
  PhysicalPlan(const unordered_map<string, shared_ptr<PhysicalOp>> &physicalOps,
               const string &rootPOpName);
  PhysicalPlan() = default;
  PhysicalPlan(const PhysicalPlan&) = default;
  PhysicalPlan& operator=(const PhysicalPlan&) = default;
  ~PhysicalPlan() = default;

  const unordered_map<string, shared_ptr<PhysicalOp>> &getPhysicalOps() const;
  const std::string& getRootPOpName() const;
  tl::expected<shared_ptr<PhysicalOp>, string> getPhysicalOp(const string &name) const;
  tl::expected<shared_ptr<PhysicalOp>, string> getRootPOp() const;
  tl::expected<void, string> renamePOp(const string oldName, const string newName);

  // last op here ignores root collate
  tl::expected<shared_ptr<PhysicalOp>, string> getLast();
  tl::expected<vector<shared_ptr<PhysicalOp>>, string> getLasts();
  tl::expected<void, string> addAsLast(shared_ptr<PhysicalOp> &op);
  tl::expected<void, string> addAsLasts(vector<shared_ptr<PhysicalOp>> &ops);

  // to fall back to pullup (adaptive pushdown), by changing FPDBFileScanPOp to RemoteFileScanPOp
  tl::expected<void, string> fallBackToPullup(const std::string &host, int port);

private:
  unordered_map<string, shared_ptr<PhysicalOp>> physicalOps_;
  string rootPOpName_;

// caf inspect
public:
  template <class Inspector>
  friend bool inspect(Inspector& f, PhysicalPlan& plan) {
    return f.object(plan).fields(f.field("physicalOps", plan.physicalOps_),
                                 f.field("rootPOpName", plan.rootPOpName_));
  }
};

}

using PhysicalPlanPtr = std::shared_ptr<fpdb::executor::physical::PhysicalPlan>;

CAF_BEGIN_TYPE_ID_BLOCK(PhysicalPlan, fpdb::caf::CAFUtil::PhysicalPlan_first_custom_type_id)
CAF_ADD_TYPE_ID(PhysicalPlan, (fpdb::executor::physical::PhysicalPlan))
CAF_END_TYPE_ID_BLOCK(PhysicalPlan)

namespace caf {
template <>
struct inspector_access<PhysicalPlanPtr> : variant_inspector_access<PhysicalPlanPtr> {
  // nop
};
} // namespace caf

#endif //FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_PHYSICALPLAN_H
