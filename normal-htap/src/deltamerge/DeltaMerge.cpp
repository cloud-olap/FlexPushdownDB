//
// Created by Han Cao on 7/15/21.
//

#include "normal/pushdown/deltamerge/DeltaMerge.h"

using namespace normal::pushdown::deltamerge;

DeltaMerge::DeltaMerge(const std::string &Name, long queryId) :
        Operator(Name, "deltamerge", queryId) {
}

std::shared_ptr <DeltaMerge> DeltaMerge::make(const std::string &Name, long queryId) {
    return std::make_shared<DeltaMerge>(Name, queryId);
}

void DeltaMerge::onReceive(const Envelope &msg) {
    if (msg.message().type() == "StartMessage") {
        this->onStart();
    } else if (msg.message().type() == "TupleMessage") {
        auto tupleMessage = dynamic_cast<const TupleMessage &>(msg.message());
        this->onTuple(tupleMessage);
    } else if (msg.message().type() == "CompleteMessage") {
        auto completeMessage = dynamic_cast<const CompleteMessage &>(msg.message());
        this->onComplete(completeMessage);
    } else {
        // FIXME: Propagate error properly
        throw std::runtime_error("Unrecognized message type " + msg.message().type());
    }
}

void DeltaMerge::onStart() {
    SPDLOG_DEBUG("Starting operator | name: '{}'", name())
}

bool DeltaMerge::allProducersComplete() {
    return ctx()->operatorMap().allComplete(OperatorRelationshipType::Producer);
}

void DeltaMerge::onTuple(const TupleMessage &message) {
    const auto &tupleSet = TupleSet2::create(message.tuples());

    if (deltaProducers_.find(message.sender()) != deltaProducers_.end()) {
        // means the sender is one of the delta producers
        deltas_.emplace_back(tupleSet);
    } else if (stableProducers_.find(message.sender()) != stableProducers_.end()) {
        stables_emplace_back(tupleSet);
    } else {
        throw std::runtime_error(fmt::format("Unrecognized producer {}, left: {}, right: {}",
                                             message.sender(), leftProducer_.lock()->name(),
                                             rightProducer_.lock()->name()));
    }

    // check if all producers complete
    if (allProducersComplete()) {
        deltaMerge();
    }
}

void DeltaMerge::onComplete(const completeMessage &) {
    // TODO: check with Yifei
    if (!ctx()->isComplete() && ctx()->operatorMap().allComplete(OperatorRelationshipType::Producer)) {
        ctx()->notifyComplete();
    }
}

void DeltaMerge::addStableProducer(const std::shared_ptr <Operator> &stableProducer) {
    stableProducers_[stableProducer.lock()->name()] = stableProducer;
    consume(stableProducer);
}

void DeltaMerge::addDeltaProducer(const std::shared_ptr <Operator> &deltaProducer) {
    deltaProducers_[deltaProducer.lock()->name()] = deltaProducer;
    consume(deltaProducer);
}

void DeltaMerge::DeltaMerge() {
    // right now assume all the tuples that we have are in arrow format
    std::vector<std::vector<>> deltaTracker;

    std::for_each(deltas_.begin(), deltas_.end()) {

    }
    return;
}



