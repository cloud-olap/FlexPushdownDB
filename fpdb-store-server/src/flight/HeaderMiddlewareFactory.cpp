//
// Created by matt on 4/2/22.
//

#include "fpdb/store/server/flight/HeaderMiddlewareFactory.hpp"
#include "fpdb/store/server/flight/HeaderMiddleware.hpp"

namespace fpdb::store::server::flight {

::arrow::Status HeaderMiddlewareFactory::StartCall(const ::arrow::flight::CallInfo& /*info*/,
                                                   const ::arrow::flight::CallHeaders& incoming_headers,
                                                   std::shared_ptr<::arrow::flight::ServerMiddleware>* middleware) {
  *middleware = std::make_shared<HeaderMiddleware>(incoming_headers);
  return ::arrow::Status::OK();
}

} // namespace fpdb::store::server::flight