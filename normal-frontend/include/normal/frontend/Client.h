//
// Created by Yifei Yang on 2/9/21.
//

#ifndef NORMAL_FRONTEND_CLIENT_H
#define NORMAL_FRONTEND_CLIENT_H

#include <normal/frontend/Global.h>
#include <normal/frontend/Config.h>
#include <normal/cache/CachingPolicy.h>
#include <normal/plan/mode/Modes.h>
#include <normal/sql/Interpreter.h>
#include <normal/core/OperatorActor.h>

using namespace normal::plan::operator_::mode;
using namespace normal::sql;
using namespace normal::core;

namespace normal::frontend {

class Client {

public:
  explicit Client();
  void processConfig();
  std::string boot();
  std::string stop();
  std::string reboot();
  std::string setCachingPolicy(int cachingPolicyType);
  std::string setMode(int modeType);
  std::string executeSql(const std::string &sql);
  std::string executeSqlFile(const std::string &filePath);

private:
  /* Config parameters */
  std::shared_ptr<Config> config_;

  std::shared_ptr<Interpreter> interpreter_;

  void configureS3ConnectorSinglePartition(std::shared_ptr<Interpreter> &i, std::string bucket_name, const std::string& dir_prefix);
  void configureS3ConnectorMultiPartition(std::shared_ptr<Interpreter> &i, std::string bucket_name, const std::string& dir_prefix);
  std::shared_ptr<TupleSet> execute();
};

std::vector<std::string> readAllRemoteIps();

}

#endif //NORMAL_FRONTEND_CLIENT_H
