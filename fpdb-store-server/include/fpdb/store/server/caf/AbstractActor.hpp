//
// Created by matt on 11/2/22.
//

#ifndef FPDB_FPDB_STORE_SERVER_INCLUDE_FPDB_STORE_SERVER_CAF_ABSTRACTACTOR_HPP
#define FPDB_FPDB_STORE_SERVER_INCLUDE_FPDB_STORE_SERVER_CAF_ABSTRACTACTOR_HPP

#include <caf/all.hpp>

#include "../Global.hpp"

namespace fpdb::store::server::caf {

using AbstractActor = ::caf::typed_actor<::caf::replies_to<::caf::ping_atom>::with<::caf::pong_atom>>;

template<class Actor>
class AbstractActorState {

public:
  AbstractActorState() = default;
  AbstractActorState(const AbstractActorState<Actor>&) = default;
  AbstractActorState& operator=(const AbstractActorState<Actor>&) = default;
  AbstractActorState(AbstractActorState<Actor>&&) noexcept = default;
  AbstractActorState& operator=(AbstractActorState<Actor>&&) noexcept = default;
  virtual ~AbstractActorState() = default;
protected:
  void set_base_state(Actor /* actor */) {
  }

  template<class... Handlers>
  auto make_base_behaviour(Actor self, Handlers... handlers) {

    self->attach_functor([=](const ::caf::error& reason) {
      if(reason) {
        SPDLOG_ERROR("Actor exited abnormally (actor name: {}, reason: {})", self->name(), to_string(reason));
      } else {
        SPDLOG_TRACE("Actor exited normally (actor name: {})", self->name());
      }

      self->state.on_exit(self, reason);
    });

    self->set_error_handler([=](const ::caf::error& error) {
      SPDLOG_ERROR("Actor error (actor name: {}, error: {})", self->name(), to_string(error));
      self->quit(error);
    });

    self->set_exit_handler([=](const ::caf::exit_msg& exit_message) {
      if(exit_message.reason != ::caf::exit_reason::normal) {

        SPDLOG_ERROR("Linked actor exited (receiving actor name: {}, source actor address: {}, reason: {})",
                     self->name(), to_string(exit_message.source), to_string(exit_message.reason));

        self->quit(::caf::make_error(
          ::caf::sec::runtime_error,
          fmt::format("Linked actor exited (receiving actor name: {}, source actor address: {}, reason: {})",
                      self->name(), to_string(exit_message.source), to_string(exit_message.reason))));
      } else {
        SPDLOG_LOGGER_TRACE(logger(),
                            "Linked actor exited (receiving actor name: {}, source actor address: {}, reason: {})",
                            self->name(), to_string(exit_message.source), to_string(exit_message.reason));
      }

      self->state.on_linked_exit(self, exit_message);
    });

    self->set_exception_handler([=](const std::exception_ptr& exception_ptr) -> ::caf::error {
      std::string message;
      try {
        if(exception_ptr) {
          std::rethrow_exception(exception_ptr);
        }
      } catch(const std::exception& exception) {
        message = exception.what();
        SPDLOG_ERROR("Unhandled exception in Actor (actor name: {}, exception: {})", self->name(), message);
      }

      return make_error(::caf::sec::runtime_error,
                        fmt::format("Unhandled exception in Actor (actor name: {}, exception: {})", self->name(),
                                    message));
    });


    return ::caf::make_typed_behavior([=](::caf::ping_atom /* atom */) { return ::caf::pong_atom_v; },
                                      std::move(handlers)...);
  }

  ///
  /// Extension points
  ///
public:

  virtual void on_exit(Actor /*self*/, const ::caf::error& /*reason*/){/* NOOP */};


  virtual void on_linked_exit(Actor /*self*/, const ::caf::exit_msg& /*exit_message*/){/* NOOP */};
};

} // namespace fpdb::store::server::caf

#endif // FPDB_FPDB_STORE_SERVER_INCLUDE_FPDB_STORE_SERVER_CAF_ABSTRACTACTOR_HPP
