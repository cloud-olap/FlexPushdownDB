//
// Created by Yifei Yang on 10/12/21.
//

#ifndef NORMAL_NORMAL_FRONTEND_INCLUDE_NORMAL_FRONTEND_CONFIG_H
#define NORMAL_NORMAL_FRONTEND_INCLUDE_NORMAL_FRONTEND_CONFIG_H

#include <normal/plan/mode/Mode.h>
#include <normal/cache/CachingPolicy.h>
#include <unordered_map>
#include <string>

using namespace normal::plan::operator_::mode;
using namespace normal::cache;

namespace normal::frontend{

class Config {
public:
  Config(const std::shared_ptr<Mode> &mode, const std::shared_ptr<CachingPolicy> &cachingPolicy, std::string s3Bucket,
         std::string s3Dir, bool showOpTimes, bool showScanMetrics);

  const std::shared_ptr<Mode> &getMode() const;
  const std::shared_ptr<CachingPolicy> &getCachingPolicy() const;
  const std::string &getS3Bucket() const;
  const std::string &getS3Dir() const;
  bool showOpTimes() const;
  bool showScanMetrics() const;

private:
  std::shared_ptr<Mode> mode_;
  std::shared_ptr<CachingPolicy> cachingPolicy_;
  std::string s3Bucket_;
  std::string s3Dir_;
  bool showOpTimes_;
  bool showScanMetrics_;

};

std::shared_ptr<Config> parseConfig();
size_t parseCacheSize(std::unordered_map<std::string, std::string> configMap);
}


#endif //NORMAL_NORMAL_FRONTEND_INCLUDE_NORMAL_FRONTEND_CONFIG_H
