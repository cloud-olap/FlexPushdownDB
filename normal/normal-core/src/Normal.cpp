//
// Created by matt on 16/12/19.
//

#include "normal/core/Normal.h"

#include "caf/all.hpp"
#include "caf/io/all.hpp"

#include "normal/core/Globals.h"

namespace normal {

caf::behavior mirror(caf::event_based_actor* self) {
  // return the (initial) actor behavior
  return {
      // a handler for messages containing a single string
      // that replies with a string
      [=](const std::string& what) -> std::string {
        // prints "Hello World!" via aout (thread-safe cout wrapper)
        aout(self) << what << std::endl;
        // reply "!dlroW olleH"
        return std::string(what.rbegin(), what.rend());
      }
  };
}

void hello_world(caf::event_based_actor* self, const caf::actor& buddy) {
  // send "Hello World!" to our buddy ...
  self->request(buddy, std::chrono::seconds(10), "Hello World!").then(
      // ... wait up to 10s for a response ...
      [=](const std::string& what) {
        // ... and print it
        aout(self) << what << std::endl;
      }
  );
}

Normal::Normal() = default;

Normal Normal::create() {
  return Normal();
}

void Normal::start() {

  spdlog::info("Normal  |  starting");

  caf::actor_system_config cfg;
  cfg.load<caf::io::middleman>();
  caf::actor_system system{cfg};

  auto mirror_actor = system.spawn(mirror);
  system.spawn(hello_world, mirror_actor);

  spdlog::info("Normal  |  started");
}

}
