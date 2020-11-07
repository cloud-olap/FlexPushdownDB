//
// Created by matt on 16/6/20.
//

#ifndef NORMAL_NORMAL_CONNECTOR_INCLUDE_NORMAL_CONNECTOR_S3_S3UTIL_H
#define NORMAL_NORMAL_CONNECTOR_INCLUDE_NORMAL_CONNECTOR_S3_S3UTIL_H

#include <string>

#include <aws/s3/S3Client.h>

using namespace Aws::S3;

namespace normal::connector::s3 {

class S3Util {

public:

  /**
   * A utility for getting the sizes of a bunch of s3 objects
   *
   * TODO: This is just the beginnings of a method of getting partition metadata for splitting up work
   *  across partitions. At the moment, just using it to get object sizes.
   */
  static std::unordered_map<std::string, long> listObjects(const std::string& s3Bucket, const std::string& prefix, const std::vector<std::string>& s3Objects, const std::shared_ptr<S3Client>& s3Client);

};

}

#endif //NORMAL_NORMAL_CONNECTOR_INCLUDE_NORMAL_CONNECTOR_S3_S3UTIL_H
