//
// Created by Yifei Yang on 1/14/22.
//

#include <fpdb/main/Server.h>
#include <csignal>

using namespace fpdb::main;

std::shared_ptr<Server> server;

int main() {
  // handle exit signals
  auto exitAct = [](int) {
    server->stop();
    server.reset();
    exit(0);
  };
  signal(SIGTERM, exitAct);
  signal(SIGINT, exitAct);
  signal(SIGABRT, exitAct);

  // start CAF server actor system
  server = std::make_shared<Server>();
  server->start();

  // make this a daemon, i.e. wait forever
  std::promise<void>().get_future().wait();
}
