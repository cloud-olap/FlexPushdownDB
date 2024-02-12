//
// Created by matt on 4/2/22.
//

#include "fpdb/store/server/flight/TicketType.hpp"
#include "fpdb/store/server/flight/Util.hpp"

namespace fpdb::store::server::flight {

TicketType::TicketType(TicketTypeId id, std::string name) : id_(id), name_(std::move(name)) {
}

std::shared_ptr<TicketType> TicketType::get_object() {
  return std::make_shared<TicketType>(TicketTypeId::GET_OBJECT, GetObjectTicketTypeName.data());
}

std::shared_ptr<TicketType> TicketType::select_object_content() {
  return std::make_shared<TicketType>(TicketTypeId::SELECT_OBJECT_CONTENT, SelectObjectContentTicketTypeName.data());
}

std::shared_ptr<TicketType> TicketType::get_bitmap() {
  return std::make_shared<TicketType>(TicketTypeId::GET_BITMAP, GetBitmapTicketTypeName.data());
}

std::shared_ptr<TicketType> TicketType::get_table() {
  return std::make_shared<TicketType>(TicketTypeId::GET_TABLE, GetTableTicketTypeName.data());
}

std::shared_ptr<TicketType> TicketType::get_batch_load_info() {
  return std::make_shared<TicketType>(TicketTypeId::GET_BATCH_LOAD_INFO, GetBatchLoadInfoTicketTypeName.data());
}

TicketTypeId TicketType::id() const {
  return id_;
}

const std::string& TicketType::name() const {
  return name_;
}

} // namespace fpdb::store::server::flight