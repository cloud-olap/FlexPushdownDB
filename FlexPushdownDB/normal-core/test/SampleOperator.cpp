//
// Created by matt on 16/9/20.
//

#include "SampleOperator.h"

#include <utility>

#include <caf/all.hpp>


SampleActor::behavior_type SampleActorFunctor(SampleActor::stateful_pointer<SampleState> self, const char* name) {
  self->state.setState(self, name);
  return self->state.makeBehavior(self);
}
