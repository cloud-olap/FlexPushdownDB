//
// Created by matt on 10/2/22.
//

#ifndef FPDB_FPDB_STORE_SERVER_INCLUDE_FPDB_STORE_SERVER_SIGNALHANDLER_HPP
#define FPDB_FPDB_STORE_SERVER_INCLUDE_FPDB_STORE_SERVER_SIGNALHANDLER_HPP

#include <functional>
#include <memory>
#include <thread>

namespace fpdb::store::server {

class SignalHandler {
public:
  explicit SignalHandler(std::function<void(int)> SignalHandlerFn);
  virtual ~SignalHandler();

  void start();
  void stop();

private:
  sigset_t sigset;
  std::function<void(int)> signal_handler_fn_;
  std::unique_ptr<std::thread> signal_handler_thread_;
};

} // namespace fpdb::store::server

#endif // FPDB_FPDB_STORE_SERVER_INCLUDE_FPDB_STORE_SERVER_SIGNALHANDLER_HPP
