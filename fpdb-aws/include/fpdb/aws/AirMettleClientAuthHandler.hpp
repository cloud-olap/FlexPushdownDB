//
// Created by matt on 4/3/22.
//

#ifndef FPDB_FPDB_AWS_INCLUDE_FPDB_AWS_AIRMETTLECLIENTAUTHHANDLER_HPP
#define FPDB_FPDB_AWS_INCLUDE_FPDB_AWS_AIRMETTLECLIENTAUTHHANDLER_HPP

#include <optional>

#include <arrow/flight/client_auth.h>

namespace fpdb::aws {

class AirMettleClientAuthHandler : public ::arrow::flight::ClientAuthHandler {
public:
  AirMettleClientAuthHandler(std::string username, std::string password, std::string application);

  ::arrow::Status Authenticate(::arrow::flight::ClientAuthSender* outgoing,
                               ::arrow::flight::ClientAuthReader* incoming) override;

  ::arrow::Status GetToken(std::string* token) override;

private:
  std::string username_;
  std::string password_;
  std::string application_;
  std::optional<std::string> token_;
};

} // namespace fpdb::aws

#endif // FPDB_FPDB_AWS_INCLUDE_FPDB_AWS_AIRMETTLECLIENTAUTHHANDLER_HPP
