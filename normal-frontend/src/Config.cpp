//
// Created by Yifei Yang on 10/12/21.
//

#include <normal/frontend/Config.h>
#include <normal/util/Util.h>
#include <normal/plan/mode/Modes.h>
#include <string>
#include <sstream>
#include <utility>
#include <normal/cache/LRUCachingPolicy.h>
#include <normal/cache/FBRCachingPolicy.h>
#include <normal/cache/FBRSCachingPolicy.h>
#include <normal/cache/WFBRCachingPolicy.h>

using namespace std;
using namespace normal::util;

namespace normal::frontend {

Config::Config(const shared_ptr<Mode> &mode, const shared_ptr<CachingPolicy> &cachingPolicy, string s3Bucket,
               string s3Dir, bool showOpTimes, bool showScanMetrics) :
               mode_(mode), cachingPolicy_(cachingPolicy), s3Bucket_(std::move(s3Bucket)), s3Dir_(std::move(s3Dir)),
               showOpTimes_(showOpTimes), showScanMetrics_(showScanMetrics) {}

const shared_ptr<Mode> &Config::getMode() const {
  return mode_;
}

const shared_ptr<CachingPolicy> &Config::getCachingPolicy() const {
  return cachingPolicy_;
}

const string &Config::getS3Bucket() const {
  return s3Bucket_;
}

const string &Config::getS3Dir() const {
  return s3Dir_;
}

bool Config::showOpTimes() const {
  return showOpTimes_;
}

bool Config::showScanMetrics() const {
  return showScanMetrics_;
}

size_t parseCacheSize(const string& stringToParse) {
  size_t cacheSize;
  if (stringToParse.substr(stringToParse.length() - 2) == "GB"
    || stringToParse.substr(stringToParse.length() - 2) == "MB"
    || stringToParse.substr(stringToParse.length() - 2) == "KB") {
    auto cacheSizeStr = stringToParse.substr(0, stringToParse.length() - 2);
    stringstream ss(cacheSizeStr);
    ss >> cacheSize;
    if (stringToParse.substr(stringToParse.length() - 2) == "GB") {
      return cacheSize * 1024 * 1024 * 1024;
    } else if (stringToParse.substr(stringToParse.length() - 2) == "MB") {
      return cacheSize * 1024 * 1024;
    } else {
      return cacheSize * 1024;
    }
  } else if (stringToParse.substr(stringToParse.length() - 1) == "B") {
    auto cacheSizeStr = stringToParse.substr(0, stringToParse.length() - 1);
    stringstream ss(cacheSizeStr);
    ss >> cacheSize;
    return cacheSize;
  }
  return 0;
}

std::shared_ptr<Mode> parseMode(const string& stringToParse) {
  if (stringToParse == "PULLUP") {
    return Modes::fullPullupMode();
  } else if (stringToParse == "PUSHDOWN_ONLY") {
    return Modes::fullPushdownMode();
  } else if (stringToParse == "CACHING_ONLY") {
    return Modes::pullupCachingMode();
  } else if (stringToParse == "HYBRID") {
    return Modes::hybridCachingMode();
  }
  return Modes::fullPullupMode();
}

std::shared_ptr<CachingPolicy> parseCachingPolicy(const string& stringToParse, size_t cacheSize,
                                                  std::shared_ptr<Mode> mode) {
  if (stringToParse == "LRU") {
    return LRUCachingPolicy::make(cacheSize, std::move(mode));
  } else if (stringToParse == "LFU") {
    return FBRCachingPolicy::make(cacheSize, std::move(mode));
  } else if (stringToParse == "LFU-S") {
    return FBRSCachingPolicy::make(cacheSize, std::move(mode));
  } else if (stringToParse == "W-LFU") {
    return WFBRCachingPolicy::make(cacheSize, std::move(mode));
  }
  return FBRSCachingPolicy::make(cacheSize, std::move(mode));
}

bool parseBool(const string& stringToParse) {
  if (stringToParse == "TRUE") {
    return true;
  } else {
    return false;
  }
}

std::shared_ptr<Config> parseConfig() {
  unordered_map<string, string> configMap = readConfig();
  size_t cacheSize = parseCacheSize(configMap["CACHE_SIZE"]);
  std::shared_ptr<Mode> mode = parseMode(configMap["MODE"]);
  std::shared_ptr<CachingPolicy> cachingPolicy = parseCachingPolicy(configMap["CACHING_POLICY"], cacheSize, mode);
  string s3Bucket = configMap["S3_BUCKET"];
  string s3Dir = configMap["S3_DIR"];
  bool showOpTimes = parseBool(configMap["SHOW_OP_TIMES"]);
  bool showScanMetrics = parseBool(configMap["SHOW_SCAN_METRICS"]);
  return std::make_shared<Config>(mode, cachingPolicy, s3Bucket, s3Dir, showOpTimes, showScanMetrics);
}
}
