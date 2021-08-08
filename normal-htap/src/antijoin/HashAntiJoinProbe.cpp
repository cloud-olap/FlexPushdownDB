//
// Created by Han Cao on 5/19/21.
//

#include "normal/pushdown/antijoin/HashAntiJoinProbe.h"

#include <utility>

#include <normal/tuple/arrow/SchemaHelper.h>

#include <normal/pushdown/Globals.h>
#include <normal/pushdown/join/ATTIC/Joiner.h>
#include <normal/tuple/TupleSetIndexWrapper.h>
#include <normal/pushdown/join/TupleSetIndexMessage.h>

using namespace normal::pushdown::antijoin;

HashAntiJoinProbe::HashAntiJoinProbe(const std::string &name, join::JoinPredicate pred,
                                     std::set<std::string> neededColumnNames, long queryId) :
        Operator(name, "HashAntiJoinProbe", queryId),
        kernel_(HashAntiJoinProbeKernel::make(std::move(pred), std::move(neededColumnNames))) {
}

HashAntiJoinProbe HashAntiJoinProbe::make(const std::string &name, join::JoinPredicate pred,
                                          std::set<std::string> neededColumnNames, long queryId) {
    return HashAntiJoinProbe(name, pred, neededColumnNames, queryId);
}

std::shared_ptr<HashAntiJoinProbe> HashAntiJoinProbe::create(const std::string &name, join::JoinPredicate pred,
                                                             std::set<std::string> neededColumnNames, long queryId) {
    return std::make_shared<HashAntiJoinProbe>(HashAntiJoinProbe(name, pred, neededColumnNames, queryId));
}

void HashAntiJoinProbe::onReceive(const normal::core::message::Envelope &msg) {

    // FIXME: Really need to get rid of these if type == string tests... Urgh

    if (msg.message().type() == "StartMessage") {
        this->onStart();
    } else if (msg.message().type() == "TupleMessage") {
        auto tupleMessage = dynamic_cast<const normal::core::message::TupleMessage &>(msg.message());
        this->onTuple(tupleMessage);
    } else if (msg.message().type() == "TupleSetIndexMessage") {
        auto hashTableMessage = dynamic_cast<const join::TupleSetIndexMessage &>(msg.message());
        this->onHashTable(hashTableMessage);
    } else if (msg.message().type() == "CompleteMessage") {
        auto completeMessage = dynamic_cast<const normal::core::message::CompleteMessage &>(msg.message());
        this->onComplete(completeMessage);
    } else {
        // FIXME: Propagate error properly
        throw std::runtime_error(fmt::format("Unrecognized message type: {}, {}", msg.message().type(), name()));
    }
}

void HashAntiJoinProbe::onStart() {
    SPDLOG_DEBUG("Starting operator  |  name: '{}'", this->name());
}

void HashAntiJoinProbe::onTuple(const normal::core::message::TupleMessage &msg) {
    // Incremental join immediately
    auto tupleSet = TupleSet2::create(msg.tuples());
    auto result = kernel_.joinProbeTupleSet(tupleSet);
    if (!result)
        throw std::runtime_error(fmt::format("{}, {}", result.error(), name()));

    // Send
    send(false);
}

void HashAntiJoinProbe::onComplete(const normal::core::message::CompleteMessage &) {
    if (!ctx()->isComplete() && ctx()->operatorMap().allComplete(normal::core::OperatorRelationshipType::Producer)) {
        send(true);
        ctx()->notifyComplete();
    }
}

void HashAntiJoinProbe::onHashTable(const join::TupleSetIndexMessage &msg) {
    // Incremental join immediately

    // TODO: We must ensure that our HashTable was fully constructed. Disable pipelining.
//    if (!ctx()->operatorMap().allComplete(normal::core::OperatorRelationshipType::Producer)) {
//        return;
//    }

    auto result = kernel_.joinBuildTupleSetIndex(msg.getTupleSetIndex());

    if (!result)
        throw std::runtime_error(fmt::format("{}, {}", result.error(), name()));

    // Send
    send(false);
}

void HashAntiJoinProbe::send(bool force) {
    auto buffer = kernel_.getBuffer();
    if (buffer.has_value() && (force || buffer.value()->numRows() >= DefaultBufferSize)) {
        std::shared_ptr<core::message::Message>
                tupleMessage = std::make_shared<core::message::TupleMessage>(buffer.value()->toTupleSetV1(), name());
        ctx()->tell(tupleMessage);
        kernel_.clear();
    }
}

