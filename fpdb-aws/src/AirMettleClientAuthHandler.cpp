//
// Created by matt on 4/3/22.
//

#include "fpdb/aws/AirMettleClientAuthHandler.hpp"

#include <arrow/flight/types.h>

namespace fpdb::aws {

AirMettleClientAuthHandler::AirMettleClientAuthHandler(std::string username,
                                                       std::string password,
                                                       std::string application)
    : username_(std::move(username)),
      password_(std::move(password)),
      application_(std::move(application)) {
}

::arrow::Status AirMettleClientAuthHandler::Authenticate(::arrow::flight::ClientAuthSender* outgoing,
                                                         ::arrow::flight::ClientAuthReader* incoming) {
  ARROW_RETURN_NOT_OK(outgoing->Write(username_));
  ARROW_RETURN_NOT_OK(outgoing->Write(password_));
  ARROW_RETURN_NOT_OK(outgoing->Write(application_));
  std::string token;
  ARROW_RETURN_NOT_OK(incoming->Read(&token));
  token_ = token;
  return ::arrow::Status::OK();
}

::arrow::Status AirMettleClientAuthHandler::GetToken(std::string* token) {
  if(!token_.has_value()) {
    return ::arrow::flight::MakeFlightError(::arrow::flight::FlightStatusCode::Unauthenticated, "Not authenticated");
  }
  *token = token_.value();
  return ::arrow::Status::OK();
}

} // namespace fpdb::aws