//
// Created by matt on 4/2/22.
//

#include "fpdb/store/server/flight/CmdType.hpp"
#include "fpdb/store/server/flight/Util.hpp"

namespace fpdb::store::server::flight {

CmdType::CmdType(CmdTypeId id, std::string name) : id_(id), name_(std::move(name)) {
}

CmdTypeId CmdType::id() const {
  return id_;
}

const std::string& CmdType::name() const {
  return name_;
}

std::shared_ptr<CmdType> CmdType::get_object() {
  return std::make_shared<CmdType>(CmdTypeId::GET_OBJECT, GetObjectCmdTypeName.data());
}

std::shared_ptr<CmdType> CmdType::select_object_content() {
  return std::make_shared<CmdType>(CmdTypeId::SELECT_OBJECT_CONTENT, SelectObjectContentCmdTypeName.data());
}

std::shared_ptr<CmdType> CmdType::put_bitmap() {
  return std::make_shared<CmdType>(CmdTypeId::PUT_BITMAP, PutBitmapCmdTypeName.data());
}

std::shared_ptr<CmdType> CmdType::clear_bitmap() {
  return std::make_shared<CmdType>(CmdTypeId::CLEAR_BITMAP, ClearBitmapCmdTypeName.data());
}

std::shared_ptr<CmdType> CmdType::put_adapt_pushdown_metrics() {
  return std::make_shared<CmdType>(CmdTypeId::PUT_ADAPT_PUSHDOWN_METRICS, PutAdaptPushdownMetricsCmdTypeName.data());
}

std::shared_ptr<CmdType> CmdType::clear_adapt_pushdown_metrics() {
  return std::make_shared<CmdType>(CmdTypeId::CLEAR_ADAPT_PUSHDOWN_METRICS, ClearAdaptPushdownMetricsCmdTypeName.data());
}

std::shared_ptr<CmdType> CmdType::set_adapt_pushdown() {
  return std::make_shared<CmdType>(CmdTypeId::SET_ADAPT_PUSHDOWN, SetAdaptPushdownCmdTypeName.data());
}

} // namespace fpdb::store::server::flight
