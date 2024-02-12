//
// Created by matt on 10/2/22.
//

#include "fpdb/store/server/SignalHandler.hpp"

#include <csignal>
#include <utility>

namespace fpdb::store::server {

SignalHandler::SignalHandler(std::function<void(int)> SignalHandlerFn)
    : signal_handler_fn_(std::move(SignalHandlerFn)) {
}

void SignalHandler::start() {

  /**
   * Set up the signal handler
   *
   * This is creating a list of signals that threads will be blocked from receiving. We also use it as the list
   * of signals we want to wait on in the handler thread
   */
  sigemptyset(&sigset);
  sigaddset(&sigset, SIGINT);
  sigaddset(&sigset, SIGHUP);
  sigaddset(&sigset, SIGQUIT);
  sigaddset(&sigset, SIGTERM);
  sigaddset(&sigset, SIGUSR2);
  pthread_sigmask(SIG_BLOCK, &sigset, nullptr);

  auto signal_handler = [&]() {
    int signum = 0;
    sigwait(&sigset, &signum);
    if(signum == SIGUSR2) {
      // We use this signal to unblock the thread and allow it to terminate
      return;
    }
    signal_handler_fn_(signum);
  };

  signal_handler_thread_ = std::make_unique<std::thread>(signal_handler);
}

void SignalHandler::stop() {
  // The thread might be stuck waiting for a signal, so we send it a bogus one just to unclog it
  if(signal_handler_thread_->joinable()) {
    pthread_kill(signal_handler_thread_->native_handle(), SIGUSR2);
    signal_handler_thread_->join();
  }
}

SignalHandler::~SignalHandler() {
  stop();
}

} // namespace fpdb::store::server