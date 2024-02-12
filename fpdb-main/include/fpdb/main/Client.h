//
// Created by Yifei Yang on 2/9/21.
//

#ifndef FPDB_FPDB_MAIN_INCLUDE_FPDB_MAIN_CLIENT_H
#define FPDB_FPDB_MAIN_INCLUDE_FPDB_MAIN_CLIENT_H

#include <fpdb/main/ExecConfig.h>
#include <fpdb/main/ActorSystemConfig.h>
#include <fpdb/executor/Executor.h>
#include <fpdb/executor/physical/PhysicalPlan.h>
#include <fpdb/calcite/CalciteClient.h>

using namespace fpdb::executor;
using namespace fpdb::executor::physical;
using namespace fpdb::calcite;
using namespace std::filesystem;

namespace fpdb::main {

class Client {

public:
  explicit Client() = default;

  static path getDefaultMetadataPath();
  static string getDefaultCatalogueName();

  string start();
  string stop();
  string restart();

  string executeQuery(const string &query);
  string executeQueryFile(const string &queryFilePath);

private:
  shared_ptr<CatalogueEntry> getCatalogueEntry(const string &schemaName);
  shared_ptr<PhysicalPlan> plan(const string &query, const shared_ptr<CatalogueEntry> &catalogueEntry);
  pair<shared_ptr<TupleSet>, long> execute(const shared_ptr<PhysicalPlan> &physicalPlan);
  void connect();

  // catalogue
  shared_ptr<Catalogue> catalogue_;

  // AWS client
  shared_ptr<AWSClient> awsClient_;

  // calcite client
  shared_ptr<CalciteClient> calciteClient_;

  // config parameters
  shared_ptr<ExecConfig> execConfig_;

  // client actor system
  shared_ptr<ActorSystemConfig> actorSystemConfig_;
  shared_ptr<::caf::actor_system> actorSystem_;

  // distributed nodes (not including the current node)
  vector<::caf::node_id> nodes_;

  // executor
  shared_ptr<Executor> executor_;

};

}

#endif //FPDB_FPDB_MAIN_INCLUDE_FPDB_MAIN_CLIENT_H
