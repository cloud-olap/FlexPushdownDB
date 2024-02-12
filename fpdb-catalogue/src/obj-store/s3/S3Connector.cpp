//
// Created by Yifei Yang on 3/1/22.
//

#include <fpdb/catalogue/obj-store/s3/S3Connector.h>

namespace fpdb::catalogue::obj_store {

S3Connector::S3Connector(const shared_ptr<AWSClient> &awsClient):
  ObjStoreConnector(ObjStoreType::S3),
  awsClient_(awsClient) {}

S3Connector::S3Connector():
  awsClient_(fpdb::aws::AWSClient::daemonClient_) {}

const shared_ptr<AWSClient> &S3Connector::getAwsClient() const {
  return awsClient_;
}

}
