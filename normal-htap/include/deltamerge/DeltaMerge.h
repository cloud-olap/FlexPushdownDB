//
// Created by Han Cao on 7/15/21.
//

#ifndef NORMAL_DELTAMERGE_H
#define NORMAL_DELTAMERGE_H

#include <normal/core/Operator.h>

#include <normal/core/message/CompleteMessage.h>
#include <normal/core/message/TupleMessage.h>
#include <normal/tuple/TupleSet2.h>
#include <string>

namespace normal::pushdown::deltamerge {
    class DeltaMerge : public core::Operator {
    public:
        explicit DeltaMerge(const std::string &Name, long queryId);

        DeltaMerge(const std::__cxx11::basic_string<char> tableName,
                   const std::__cxx11::basic_string<char> &Name,
                   long queryId,
                   std::shared_ptr<::arrow::Schema> outputSchema,
                   ))

        DeltaMerge(const std::__cxx11::basic_string<char> tableName, const std::__cxx11::basic_string<char> &Name,
                   long queryId);

        static std::shared_ptr <DeltaMerge> make(const std::string &Name, long queryId);

        void onReceive(const core::message::Envelope &msg) override;

        void onStart();

        void onComplete(const core::message::CompleteMessage &);

        void onTuple(const core::message::TupleMessage &message);


    private:
        std::string tableName_;

        std::shared_ptr<::arrow::Schema> outputSchema_;

        int primaryKeyColumnNums_;
        std::unordered_map<std::string, std::weak_ptr<Operator>> deltaProducers_;
        std::unordered_map<std::string, std::weak_ptr<Operator>> stableProducers_;

        std::vector<std::vector<std::shared_ptr<Column>>> deltaTracker_;
        std::vector<std::vector<std::shared_ptr<Column>>> stableTracker_;

        std::vector<int> deltaIndexTracker_;
        std::vector<int> stableIndexTracker_;

        std::list <std::shared_ptr<TupleSet2>> deltas_;
        std::list <std::shared_ptr<TupleSet2>> stables_;

        std::vector<std::array<int,2>> deleteMap_;

        void deltaMerge();

        void addStableProducer(const std::shared_ptr<Operator> &stableProducer);

        void addDeltaProducer(const std::shared_ptr<Operator> &deltaProducer);

        bool allProducersComplete();

        void populateArrowTrackers();

        bool checkIfAllRecordsWereVisited();

        void generateDeleteMaps();

        void generateFinalResult();
    }

}


#endif //NORMAL_DELTAMERGE_H