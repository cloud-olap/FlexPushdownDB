//
// Created by matt on 4/2/22.
//

#include "fpdb/store/server/flight/HeaderMiddleware.hpp"

namespace fpdb::store::server::flight {

std::string HeaderMiddleware::name() const {
  return "HeaderMiddleware";
}

void HeaderMiddleware::SendingHeaders(::arrow::flight::AddCallHeaders* /*outgoing_headers*/) {
  // NOOP
}

void HeaderMiddleware::CallCompleted(const ::arrow::Status& /*status*/) {
  // NOOP
}

HeaderMiddleware::HeaderMiddleware(::arrow::flight::CallHeaders headers) : headers_(std::move(headers)) {
}

const ::arrow::flight::CallHeaders& HeaderMiddleware::headers() const {
  return headers_;
}

} // namespace fpdb::store::server::flight