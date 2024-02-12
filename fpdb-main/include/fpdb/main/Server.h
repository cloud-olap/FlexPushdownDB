//
// Created by Yifei Yang on 1/14/22.
//

#ifndef FPDB_FPDB_MAIN_INCLUDE_FPDB_MAIN_SERVER_H
#define FPDB_FPDB_MAIN_INCLUDE_FPDB_MAIN_SERVER_H

#include <fpdb/main/ActorSystemConfig.h>

namespace fpdb::main {

class Server {

public:
  explicit Server() = default;

  void start();
  void stop();

private:
  std::shared_ptr<ActorSystemConfig> actorSystemConfig_;
  std::shared_ptr<::caf::actor_system> actorSystem_;
  std::future<tl::expected<void, std::basic_string<char>>> flight_future_;

};

}


#endif //FPDB_FPDB_MAIN_INCLUDE_FPDB_MAIN_SERVER_H
