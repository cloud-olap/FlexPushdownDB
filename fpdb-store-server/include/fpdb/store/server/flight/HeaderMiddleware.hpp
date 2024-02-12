//
// Created by matt on 4/2/22.
//

#ifndef FPDB_FPDB_STORE_SERVER_INCLUDE_FPDB_STORE_SERVER_FLIGHT_HEADERMIDDLEWARE_HPP
#define FPDB_FPDB_STORE_SERVER_INCLUDE_FPDB_STORE_SERVER_FLIGHT_HEADERMIDDLEWARE_HPP

#include <arrow/flight/api.h>

namespace fpdb::store::server::flight {

class HeaderMiddleware : public ::arrow::flight::ServerMiddleware {
public:
  explicit HeaderMiddleware(::arrow::flight::CallHeaders headers);

  [[nodiscard]] std::string name() const override;
  void SendingHeaders(::arrow::flight::AddCallHeaders* outgoing_headers) override;
  void CallCompleted(const ::arrow::Status& status) override;

  [[nodiscard]] const ::arrow::flight::CallHeaders& headers() const;

private:
  ::arrow::flight::CallHeaders headers_;
};

} // namespace fpdb::store::server::flight

#endif // FPDB_FPDB_STORE_SERVER_INCLUDE_FPDB_STORE_SERVER_FLIGHT_HEADERMIDDLEWARE_HPP
