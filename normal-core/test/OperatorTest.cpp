//
// Created by matt on 16/9/20.
//

#include <doctest/doctest.h>

#include <caf/all.hpp>

#include "SampleOperator.h"

using namespace caf;

TEST_SUITE ("operator") {

TEST_CASE ("operator-spawn" * doctest::skip(false)) {

//  actor_system_config cfg;
//  actor_system system{cfg};
//
//  auto cacheLoad = system.spawn(SampleActorFunctor, "cache-load");
//  auto scan = system.spawn(SampleActorFunctor, "scan");
//  auto collate = system.spawn(SampleActorFunctor, "collate");
//
//  {
//	scoped_actor self{system};
//
//	self->anon_send(cacheLoad, ConnectAtom::value, std::vector<OperatorConnection>{OperatorConnection("scan", actor_cast<actor>(scan), OperatorRelationshipType::Consumer)});
//	self->anon_send(scan, ConnectAtom::value, std::vector<OperatorConnection>{OperatorConnection("collate", actor_cast<actor>(collate), OperatorRelationshipType::Consumer)});
//	self->anon_send(collate, ConnectAtom::value, std::vector<OperatorConnection>{});
//	self->anon_send(cacheLoad, StopAtom::value);
//	self->anon_send(cacheLoad, CompleteAtom::value);
//	self->anon_send(cacheLoad, add_atom::value);
//
//	self->send_exit(cacheLoad, exit_reason::user_shutdown);
//	self->send_exit(scan, exit_reason::user_shutdown);
//	self->send_exit(collate, exit_reason::user_shutdown);
//  }
//
//  system.await_all_actors_done();


}

}
