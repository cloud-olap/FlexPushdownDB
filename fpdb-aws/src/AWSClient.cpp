//
// Created by matt on 5/3/20.
//

#include <fpdb/aws/AWSClient.h>
#include <spdlog/spdlog.h>
#include <filesystem>

#include "fpdb/aws/ProfileAWSCredentialsProviderChain.h"
#include "fpdb/aws/AirMettleClientAuthHandler.hpp"
#include <fpdb/util/Util.h>
#include <arrow/flight/types.h>
#include <arrow/flight/client.h>

namespace fpdb::aws {

AWSClient::AWSClient(const shared_ptr<AWSConfig> &awsConfig) :
  awsConfig_(awsConfig) {
}

void AWSClient::init() {
  options_.loggingOptions.logLevel = Aws::Utils::Logging::LogLevel::Off;
  Aws::InitAPI(options_);
  s3Client_ = makeS3Client();
}

[[maybe_unused]] void AWSClient::shutdown() {
  Aws::ShutdownAPI(options_);
}

std::shared_ptr<Aws::S3::S3Client> AWSClient::makeS3Client() {
  static const char *ALLOCATION_TAG = "Normal";

  std::shared_ptr<Aws::S3::S3Client> s3Client;

  Aws::Client::ClientConfiguration config;
  config.scheme = Aws::Http::Scheme::HTTP;
  config.region = Aws::Region::US_EAST_2;

  // This value has been tuned for c5a.4xlarge, c5a.8xlarge, and c5n.9xlarge, any more connections than this and aggregate
  // network performance degrades rather than remaining constant
  // This value makes low selectivity S3 Select requests much faster as it utilizes more network bandwidth for them.
  // S3 Select requests with high selectivity are unaffected, as are GET requests (so for GET we don't run it in parallel)
  config.maxConnections = 200; // Default = 25
  config.retryStrategy = Aws::MakeShared<Aws::Client::DefaultRetryStrategy>(ALLOCATION_TAG, 0, 0); // Disable retries
  config.connectTimeoutMs = 500000;
  config.requestTimeoutMs = 900000;
  // Default is to create and dispatch to thread with async methods, we don't use async so default is ideal
  config.executor = Aws::MakeShared<Aws::Utils::Threading::DefaultExecutor>(ALLOCATION_TAG);
  config.verifySSL = false;

  // Commented this out as turning it off increase transfer rate. It appears that having this set
  // reduces transfer rate even if the chosen value is very high.
  if (awsConfig_->getNetworkLimit() > 0) {
    std::shared_ptr<Aws::Utils::RateLimits::RateLimiterInterface> limiter;
    limiter = Aws::MakeShared<Aws::Utils::RateLimits::DefaultRateLimiter<>>(ALLOCATION_TAG, awsConfig_->getNetworkLimit());
    config.readRateLimiter = limiter;
    config.writeRateLimiter = limiter;
  }

  switch (awsConfig_->getS3ClientType()) {
    case S3: {
      SPDLOG_DEBUG("Using S3 Client");
      s3Client = Aws::MakeShared<Aws::S3::S3Client>(
              ALLOCATION_TAG,
              Aws::MakeShared<fpdb::aws::ProfileAWSCredentialsProviderChain>(ALLOCATION_TAG, "FlexPushdownDB"),
              config,
              Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never,
              true);
      break;
    }
    case AIRMETTLE: {
      SPDLOG_DEBUG("Using Airmettle Client");

      auto resources_dir = std::filesystem::current_path().append("resources");
      auto cfg = util::readConfig("aws.conf");

      auto s3_endpoint_override_it = cfg.find("s3.endpoint-override");
      if(s3_endpoint_override_it == cfg.end()){
        throw std::runtime_error("No s3.endpoint-override found in config");
      }
      auto s3_endpoint_override = s3_endpoint_override_it->second;

      auto s3_access_key_it = cfg.find("s3.access-key");
      if(s3_access_key_it == cfg.end()){
        throw std::runtime_error("No s3.access-key found in config");
      }
      auto s3_access_key = s3_access_key_it->second;

      auto s3_secret_key_it = cfg.find("s3.secret-key");
      if(s3_access_key_it == cfg.end()){
        throw std::runtime_error("No s3.secret-key found in config");
      }
      auto s3_secret_key = s3_secret_key_it->second;

      config.endpointOverride = s3_endpoint_override; //"54.151.121.20/s3/test";
      Aws::String accessKeyId = Aws::String(s3_access_key); //"test-test";
      Aws::String secretKey = Aws::String(s3_secret_key); //"test";
      Aws::Auth::AWSCredentials airmettleCredentials = Aws::Auth::AWSCredentials(accessKeyId, secretKey);

      s3Client = Aws::MakeShared<Aws::S3::S3Client>(
              ALLOCATION_TAG,
              airmettleCredentials,
              config,
              Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never,
              false);

      /**
       * Open a flight connection for Airmettle as well if flight.enabled, flight.host, flight.port defined
       * in config
       */
      auto flight_enabled = false;
      auto flight_enabled_it =  cfg.find("flight.enabled");
      if(flight_enabled_it != cfg.end() && flight_enabled_it->second == "true") {
        flight_enabled = true;
      }

      if(flight_enabled){
          auto flight_host_it = cfg.find("flight.host");
          auto flight_port_it = cfg.find("flight.port");
          auto flight_username_it = cfg.find("flight.username");
          auto flight_password_it = cfg.find("flight.password");
          auto flight_application_it = cfg.find("flight.application");

          if(flight_host_it == cfg.end()){
            throw std::runtime_error("No flight.host found in config");
          }
          auto flight_host = flight_host_it->second;

          if(flight_port_it == cfg.end()){
            throw std::runtime_error("No flight.port found in config");
          }
          auto flight_port = stoi(flight_port_it->second);

          if(flight_username_it == cfg.end()){
            throw std::runtime_error("No flight.username found in config");
          }
          auto flight_username = flight_username_it->second;

          if(flight_password_it == cfg.end()){
            throw std::runtime_error("No flight.password found in config");
          }
          auto flight_password = flight_password_it->second;

          if(flight_application_it == cfg.end()){
            throw std::runtime_error("No flight.application found in config");
          }
          auto flight_application = flight_application_it->second;

          // Connect
          arrow::flight::Location client_location;
          ::arrow::Status sts = arrow::flight::Location::ForGrpcTcp(flight_host, flight_port, &client_location);
          if(!sts.ok()){
            throw std::runtime_error(sts.message());
          }
          arrow::flight::FlightClientOptions client_options = arrow::flight::FlightClientOptions::Defaults();
          std::unique_ptr<::arrow::flight::FlightClient> flight_client;
          sts = arrow::flight::FlightClient::Connect(client_location, client_options, &flight_client);
          if(!sts.ok()){
            throw std::runtime_error(sts.message());
          }

          // Authenticate
          arrow::flight::FlightCallOptions call_options;
          std::unique_ptr<::arrow::flight::ClientAuthHandler> client_auth_handler =
            std::make_unique<AirMettleClientAuthHandler>(flight_username, flight_password, flight_application);
          sts = flight_client->Authenticate(call_options, std::move(client_auth_handler));
          if(!sts.ok()){
            throw std::runtime_error(sts.message());
          }

          flight_client_ = std::move(flight_client);
      }

      break;
    }
    case MINIO: {
      SPDLOG_DEBUG("Using Minio Client");
      config.endpointOverride = "172.31.10.231:9000";
      Aws::String accessKeyId = "minioadmin";
      Aws::String secretKey = "minioadmin";
      Aws::Auth::AWSCredentials minioCredentials = Aws::Auth::AWSCredentials(accessKeyId, secretKey);

      s3Client = Aws::MakeShared<Aws::S3::S3Client>(
              ALLOCATION_TAG,
              minioCredentials,
              config,
              Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never,
              false);
      break;
    }
    default: {
      throw std::runtime_error("Bad S3Client Type");
    }
  }

  return s3Client;
}

const shared_ptr<AWSConfig> &AWSClient::getAwsConfig() const {
  return awsConfig_;
}

const shared_ptr<S3Client> &AWSClient::getS3Client() const {
  return s3Client_;
}

const std::optional<std::unique_ptr<::arrow::flight::FlightClient>>& AWSClient::getFlightClient() const {
  return flight_client_;
}

}
