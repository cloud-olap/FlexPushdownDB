//
// Created by Yifei Yang on 10/30/21.
//

#include <fpdb/calcite/CalciteConfig.h>
#include <fpdb/util/Util.h>
#include <unordered_map>

using namespace std;
using namespace fpdb::util;

namespace fpdb::calcite {

CalciteConfig::CalciteConfig(int port, string jarName) :
  port_(port),
  jarName_(std::move(jarName)) {}

int CalciteConfig::getPort() const {
  return port_;
}

const string &CalciteConfig::getJarName() const {
  return jarName_;
}

std::shared_ptr<CalciteConfig> CalciteConfig::parseCalciteConfig() {
  unordered_map<string, string> configMap = readConfig("calcite.conf");
  int port = stoi(configMap["SERVER_PORT"]);
  string jarName = configMap["JAR_NAME"];
  return make_shared<CalciteConfig>(port, jarName);
}

}
