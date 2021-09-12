//
// Created by Han Cao on 7/15/21.
//

#include <deltamerge/DeltaMerge.h>
#include <normal/connector/MiniCatalogue.h>
#include <string>
#include <normal/tuple/ArrayAppender.h>
#include <normal/tuple/ArrayAppenderWrapper.h>
#include <normal/tuple/ColumnBuilder.h>

using namespace normal::pushdown::deltamerge;

DeltaMerge::DeltaMerge(const std::string tableName, const std::string &Name, long queryId) :
        Operator(Name, "deltamerge", queryId) {
    tableName_ = tableName;
}

DeltaMerge::DeltaMerge(const std::string tableName, const std::string &Name, long queryId, std::shared_ptr<::arrow::Schema> outputSchema) :
Operator(Name, "deltamerge", queryId),
outputSchema_(std::move(outputSchema)){
    tableName_ = tableName;

}

DeltaMerge::DeltaMerge(const std::string &Name, long queryId) :
        Operator(Name, "deltamerge", queryId) {
}

std::shared_ptr <DeltaMerge> DeltaMerge::make(const std::string &Name, long queryId) {
    return std::make_shared<DeltaMerge>(Name, queryId);
}

void DeltaMerge::onReceive(const core::message::Envelope &msg) {
    if (msg.message().type() == "StartMessage") {
        this->onStart();
    } else if (msg.message().type() == "TupleMessage") {
        auto tupleMessage = dynamic_cast<const core::message::TupleMessage &>(msg.message());
        this->onTuple(tupleMessage);
    } else if (msg.message().type() == "CompleteMessage") {
        auto completeMessage = dynamic_cast<const core::message::CompleteMessage &>(msg.message());
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
    return ctx()->operatorMap().allComplete(core::OperatorRelationshipType::Producer);
}

void DeltaMerge::onTuple(const core::message::TupleMessage &message) {
    const auto &tupleSet = TupleSet2::create(message.tuples());

    if (deltaProducers_.find(message.sender()) != deltaProducers_.end()) {
        // means the sender is one of the delta producers
        deltas_.emplace_back(tupleSet);
    } else if (stableProducers_.find(message.sender()) != stableProducers_.end()) {
        stables_.emplace_back(tupleSet);
    } else {
//        throw std::runtime_error(fmt::format("Unrecognized producer {}, left: {}, right: {}",
//                                             message.sender(), leftProducer_.lock()->name(),
//                                             rightProducer_.lock()->name()));
    }

    // check if all producers complete
    if (allProducersComplete()) {
        deltaMerge();
    }
}

void DeltaMerge::onComplete(const core::message::CompleteMessage &) {
    // TODO: check with Yifei
    if (!ctx()->isComplete() && ctx()->operatorMap().allComplete(core::OperatorRelationshipType::Producer)) {
        ctx()->notifyComplete();
    }
}

void DeltaMerge::addStableProducer(const std::shared_ptr <Operator> &stableProducer) {
    stableProducers_[stableProducer->name()] = stableProducer;
    consume(stableProducer);
}

void DeltaMerge::addDeltaProducer(const std::shared_ptr <Operator> &deltaProducer) {
    deltaProducers_[deltaProducer->name()] = deltaProducer;
    consume(deltaProducer);
}

void DeltaMerge::populateArrowTrackers() {

    // right now assume all the tuples that we have are in arrow format
    auto miniCatalougue = normal::connector::defaultMiniCatalogue;

    // set up a process to obtain the needed columns (Primary Keys, Timestamp, Type)
    // FIXME: SUPPORT Composited PrimaryKey
    std::vector<std::string> primaryKeys;
    std::string pk = miniCatalougue->getPrimaryKeyColumnName(tableName_);
    primaryKeys.emplace_back(pk);

    // For deltas, we obtain three things: primary key, timestamp, type
    for (auto delta = std::begin(deltas_); delta != std::end(deltas_); ++delta) {
        std::vector<std::shared_ptr<Column>> columnTracker;
        for (auto key : primaryKeys) {
            auto keyColumn = delta->get()->getColumnByName(key);
            columnTracker.emplace_back(keyColumn.value());
        }
        auto timestampColumn = delta->get()->getColumnByName("timestamp");
        auto typeColumn = delta->get()->getColumnByName("type");
        columnTracker.emplace_back(timestampColumn.value());
        columnTracker.emplace_back(typeColumn.value());

        deltaTracker_.emplace_back(columnTracker);
    }

    // For stables, we only obtaining the primary key
    for (auto stable = std::begin(stables_); stable != std::end(stables_); ++stable) {
        std::vector<std::shared_ptr<Column>> columnTracker;
        for (auto key : primaryKeys) {
            auto keyColumn = stable->get()->getColumnByName(key);
            columnTracker.emplace_back(keyColumn.value());
        }

        stableTracker_.emplace_back(columnTracker);
    }
}

/**
 * Determine which records to copy.
 */
void DeltaMerge::generateDeleteMaps() {

    // FIXME: we do not consider composited primaryKey situation for now
    std::vector<int> stablePKStates;
    std::vector<int> deltaPKStates;

    while (checkIfAllRecordsWereVisited()) {
        // loop through the stable and find the primary key
        std::string currPK;
        for (int i = 0; i < stableTracker_.size(); i++) {
            currPK = std::min(currPK, stableTracker_[i][0]->element(deltaIndexTracker_[i]).value()->toString());
        }
        for (int i = 0; i < deltaTracker_.size(); i++) {
            currPK = std::min(currPK, deltaTracker_[i][0]->element(deltaIndexTracker_[i]).value()->toString());
        }
        // now you get the smallest primary key
        // 1. Select all the deltas with the current primary key
        // 1.5 compare the timestamp and get one tuple only
        std::string minTS = "0";

        std::array<int, 2> position = {0, 0}; // [deltaNum, idx]

        for (int i = 0; i < stableTracker_.size(); i++) {
            if (currPK == stableTracker_[i][0]->element(stableIndexTracker_[i]).value()->toString()) {
                position[0] = i;
                position[1] = stableIndexTracker_[i];

                stableIndexTracker_[i] = stableIndexTracker_[i] + 1;
            }
        }

        // FIXME: Where does the stable fit in?
        for (int i = 0; i < deltaTracker_.size(); i++) {
            if (currPK != deltaTracker_[i][0]->element(deltaIndexTracker_[i]).value()->toString()) continue;
            int tsIndex = deltaTracker_[0].size() - 2;
            auto currTS = deltaTracker_[i][tsIndex]->element(deltaIndexTracker_[i]).value()->toString();

            deltaIndexTracker_[i] = deltaIndexTracker_[i] + 1;

            if (currTS < minTS) {
                // update position
                minTS = currTS;
                position[0] = stableTracker_.size() + i - 1;
                position[1] = deltaIndexTracker_[i];
            }
        }
        // TODO: insert the result into the delete map

        if (deleteMap_.find(position[0]) == deleteMap_.end()) {  // did not find the key
            std::unordered_set<int> temp = {position[1]};
            deleteMap_[position[0]] = temp;
        } else
            deleteMap_[position[0]].insert(position[1]);
        }
    }
}

/**
 * Based on the deleteMap, copy the needed values to a new TupleSet
 */
void DeltaMerge::generateFinalResult() {

    // initialize an array of column appender
    std::vector<std::shared_ptr<ColumnBuilder>> columnBuilderArray;
    for (int i = 0; i < outputSchema_->num_fields(); i++) {
        std::string newColumnBuilderName = outputSchema_->field_names()[i] + "columnBuilder";
        auto newColumnBuilder = ColumnBuilder::make(newColumnBuilderName, outputSchema_->field(i)->type());
    }

    // We first try to append the stable data to the new table
    for (int i = 0; i < stableTracker_.size(); i++) {
        auto deleteSet = deleteMap_.at(i); // get the deleteMap for this file
        auto originalTable  = stables_[i]; // Get the original stable file

        // now we try to loop through the entire file
        for (size_t r = 0; r < originalTable->numRows(); r++) {
            if (deleteSet.find(r) == deleteSet.end()) continue;
            // if the row is found, then copy it one column by one column
            for (size_t c = 0; c < outputSchema_->num_fields(); c++) {
                columnBuilderArray[c]->append(originalTable->getColumnByIndex(c).value()->element(r).value());
            }
        }
    }

    // Do the same thing again to the deltas
    for (int i = 0; i < deltaTracker_.size(); i++) {
        int offsetted_i = i + stableTracker_.size();
        auto deleteSet = deleteMap_.at(offsetted_i); // get the deleteMap for this file
        auto originalTable  = deltas_[i]; // Get the original stable file

        // now we try to loop through the entire file
        for (size_t r = 0; r < originalTable->numRows(); r++) {
            if (deleteSet.find(r) == deleteSet.end()) continue;
            // if the row is found, then copy it one column by one column
            for (size_t c = 0; c < outputSchema_->num_fields(); c++) {
                columnBuilderArray[c]->append(originalTable->getColumnByIndex(c).value()->element(r).value());
            }
        }
    }

    std::vector<std::shared_ptr<Column>> builtColumns;
    // now we try to generate the final output
    for (auto & i : columnBuilderArray){
        builtColumns.emplace_back(i->finalize());
    }

    auto finalOutput = TupleSet2::make(normal::tuple::Schema::make(outputSchema_), builtColumns);

}

/**
 *
 * @return true if not visited, false if visited all
 */
bool DeltaMerge::checkIfAllRecordsWereVisited() {
    for (int i = 0; i < deltaTracker_.size();i++) {
        if (deltaIndexTracker_[i] < deltaTracker_[i][0]->numRows() - 1) {
            return true;
        }

        if (stableIndexTracker_[i] < stableTracker_[i][0]->numRows() - 1) {
            return true;
        }
    }

    return false;
}

void DeltaMerge::deltaMerge() {

    return;
}