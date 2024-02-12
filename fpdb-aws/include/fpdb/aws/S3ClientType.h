//
// Created by Yifei Yang on 11/10/21.
//

#ifndef FPDB_FPDB_AWS_INCLUDE_FPDB_AWS_S3CLIENTTYPE_H
#define FPDB_FPDB_AWS_INCLUDE_FPDB_AWS_S3CLIENTTYPE_H

namespace fpdb::aws {

enum S3ClientType {
  S3,
  AIRMETTLE,
  MINIO
};

}

#endif //FPDB_FPDB_AWS_INCLUDE_FPDB_AWS_S3CLIENTTYPE_H
