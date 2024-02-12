//
// Created by Yifei Yang on 12/13/22.
//

#ifndef FPDB_FPDB_MAIN_TEST_BASE_ADAPTPUSHDOWNTESTUTIL_H
#define FPDB_FPDB_MAIN_TEST_BASE_ADAPTPUSHDOWNTESTUTIL_H

#include <fpdb/store/server/Server.hpp>
#include <fpdb/store/server/flight/CmdObject.hpp>
#include <fpdb/store/client/FPDBStoreClientConfig.h>
#include <unordered_map>
#include <string>

namespace fpdb::main::test {

class AdaptPushdownTestUtil {

public:
  static void run_adapt_pushdown_benchmark_query(const std::string &schemaName,
                                                 const std::string &queryFileName,
                                                 const std::vector<int> &maxThreadsVec,
                                                 int parallelDegree,
                                                 bool startFPDBStore);

private:
  static void set_pushdown_flags(bool enableAdaptPushdown, int maxThreads,
                                 bool isFPDBStoreRemote);

  static void send_cmd_to_storage(const std::shared_ptr<fpdb::store::server::flight::CmdObject> &cmdObj);
};

}


#endif //FPDB_FPDB_MAIN_TEST_BASE_ADAPTPUSHDOWNTESTUTIL_H
