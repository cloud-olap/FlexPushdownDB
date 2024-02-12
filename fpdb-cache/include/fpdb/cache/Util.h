//
// Created by Yifei Yang on 11/15/21.
//

#ifndef FPDB_FPDB_CACHE_INCLUDE_FPDB_CACHE_UTIL_H
#define FPDB_FPDB_CACHE_INCLUDE_FPDB_CACHE_UTIL_H

#include <fpdb/cache/SegmentKey.h>
#include <unordered_map>
#include <filesystem>

using namespace std;

namespace fpdb::cache {

class Util {
public:
  static unordered_map<shared_ptr<SegmentKey>, size_t, cache::SegmentKeyPointerHash, cache::SegmentKeyPointerPredicate>
    readSegmentKeySize(const std::string& s3Bucket,
                       const std::string& schemaName,
                       filesystem::path &filePath);
};

}


#endif //FPDB_FPDB_CACHE_INCLUDE_FPDB_CACHE_UTIL_H
