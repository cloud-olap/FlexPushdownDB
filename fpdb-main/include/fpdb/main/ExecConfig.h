//
// Created by Yifei Yang on 10/12/21.
//

#ifndef FPDB_FPDB_MAIN_INCLUDE_FPDB_MAIN_EXECCONFIG_H
#define FPDB_FPDB_MAIN_INCLUDE_FPDB_MAIN_EXECCONFIG_H

#include <fpdb/cache/policy/CachingPolicy.h>
#include <fpdb/plan/Mode.h>
#include <fpdb/catalogue/Catalogue.h>
#include <fpdb/catalogue/obj-store/ObjStoreConnector.h>
#include <fpdb/aws/AWSClient.h>
#include <unordered_map>
#include <string>

using namespace fpdb::cache::policy;
using namespace fpdb::plan;
using namespace fpdb::catalogue;
using namespace fpdb::catalogue::obj_store;
using namespace fpdb::aws;

namespace fpdb::main{

class ExecConfig {
public:
  ExecConfig(const shared_ptr<Mode> &mode,
             const shared_ptr<CachingPolicy> &cachingPolicy,
             string s3Bucket,
             string schemaName,
             int parallelDegree,
             bool showOpTimes,
             bool showScanMetrics,
             int CAFServerPort,
             bool isDistributed);

  static shared_ptr<ExecConfig> parseExecConfig(const shared_ptr<Catalogue> &catalogue,
                                                const shared_ptr<ObjStoreConnector> &objStoreConnector);
  static int parseCAFServerPort();
  static int parseFlightPort();

  const shared_ptr<Mode> &getMode() const;
  const shared_ptr<CachingPolicy> &getCachingPolicy() const;
  const string &getS3Bucket() const;
  const string &getSchemaName() const;
  int getParallelDegree() const;
  bool showOpTimes() const;
  bool showScanMetrics() const;
  int getCAFServerPort() const;
  bool isDistributed() const;

private:
  static size_t parseCacheSize(const string& stringToParse);
  static shared_ptr<Mode> parseMode(const string& stringToParse);
  static shared_ptr<CachingPolicy> parseCachingPolicy(const string& stringToParse,
                                                      size_t cacheSize,
                                                      const shared_ptr<CatalogueEntry> &catalogueEntry);

  shared_ptr<Mode> mode_;
  shared_ptr<CachingPolicy> cachingPolicy_;
  string s3Bucket_;
  string schemaName_;
  int parallelDegree_;
  bool showOpTimes_;
  bool showScanMetrics_;
  int CAFServerPort_;
  bool isDistributed_;

};

}


#endif //FPDB_FPDB_MAIN_INCLUDE_FPDB_MAIN_EXECCONFIG_H
