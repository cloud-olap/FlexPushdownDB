//
// Created by Yifei Yang on 3/1/22.
//

#ifndef FPDB_FPDB_CATALOGUE_INCLUDE_FPDB_CATALOGUE_OBJ_STORE_S3_S3CONNECTOR_H
#define FPDB_FPDB_CATALOGUE_INCLUDE_FPDB_CATALOGUE_OBJ_STORE_S3_S3CONNECTOR_H

#include <fpdb/catalogue/obj-store/ObjStoreConnector.h>
#include <fpdb/aws/AWSClient.h>

using namespace fpdb::aws;

namespace fpdb::catalogue::obj_store {

class S3Connector: public ObjStoreConnector {

public:
  S3Connector(const shared_ptr<AWSClient> &awsClient);
  S3Connector();
  S3Connector(const S3Connector&) = default;
  S3Connector& operator=(const S3Connector&) = default;
  ~S3Connector() = default;

  const shared_ptr<AWSClient> &getAwsClient() const;

private:
  shared_ptr<AWSClient> awsClient_;

// caf inspect
public:
  template <class Inspector>
  friend bool inspect(Inspector& f, S3Connector& conn) {
    return f.object(conn).fields(f.field("storeType", conn.storeType_));
  }
};

}


#endif //FPDB_FPDB_CATALOGUE_INCLUDE_FPDB_CATALOGUE_OBJ_STORE_S3_S3CONNECTOR_H
