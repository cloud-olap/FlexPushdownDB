//
// Created by Yifei Yang on 4/20/22.
//

#include <fpdb/executor/physical/group/GroupArrowKernel.h>
#include <fpdb/executor/physical/group/GroupKernel.h>

namespace fpdb::executor::physical::group {

GroupAbstractKernel::GroupAbstractKernel(GroupKernelType type,
                                         const std::vector<std::string> &groupColumnNames,
                                         const std::vector<std::shared_ptr<AggregateFunction>> &aggregateFunctions):
  type_(type),
  groupColumnNames_(ColumnName::canonicalize(groupColumnNames)),
  aggregateFunctions_(aggregateFunctions) {}

GroupKernelType GroupAbstractKernel::getType() const {
  return type_;
}

::nlohmann::json GroupAbstractKernel::toJson() const {
  ::nlohmann::json jObj;
  jObj.emplace("type", type_);
  jObj.emplace("groupColumnNames", groupColumnNames_);

  vector<::nlohmann::json> functionsJArr;
  for (const auto &function: aggregateFunctions_) {
    functionsJArr.emplace_back(function->toJson());
  }
  jObj.emplace("aggregateFunctions", functionsJArr);
  return jObj;
}

tl::expected<std::shared_ptr<GroupAbstractKernel>, std::string>
GroupAbstractKernel::fromJson(const nlohmann::json &jObj) {
  if (!jObj.contains("type")) {
    return tl::make_unexpected(fmt::format("Type not specified in group abstract kernel JSON '{}'", to_string(jObj)));
  }
  auto type = jObj["type"].get<GroupKernelType>();

  if (!jObj.contains("groupColumnNames")) {
    return tl::make_unexpected(fmt::format("GroupColumnNames not specified in group abstract kernel JSON '{}'", to_string(jObj)));
  }
  auto groupColumnNames = jObj["groupColumnNames"].get<std::vector<std::string>>();

  if (!jObj.contains("aggregateFunctions")) {
    return tl::make_unexpected(fmt::format("AggregateFunctions not specified in group abstract kernel JSON '{}'", to_string(jObj)));
  }
  auto functionsJArr = jObj["aggregateFunctions"].get<std::vector<::nlohmann::json>>();
  std::vector<std::shared_ptr<AggregateFunction>> aggregateFunctions;
  for (const auto &functionJObj: functionsJArr) {
    auto expFunction = AggregateFunction::fromJson(functionJObj);
    if (!expFunction.has_value()) {
      return tl::make_unexpected(expFunction.error());
    }
    aggregateFunctions.emplace_back(*expFunction);
  }

  switch (type) {
    case GroupKernelType::GROUP_ARROW_KERNEL: {
      return std::make_shared<GroupArrowKernel>(groupColumnNames, aggregateFunctions);
    }
    case GroupKernelType::GROUP_KERNEL: {
      return std::make_shared<GroupKernel>(groupColumnNames, aggregateFunctions);
    }
    default:
      return tl::make_unexpected(fmt::format("Unknown type in group abstract kernel JSON '{}'", to_string(jObj)));
  }
}

}
