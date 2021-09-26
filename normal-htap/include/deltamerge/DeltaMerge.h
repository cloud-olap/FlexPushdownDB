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

namespace normal::htap::deltamerge {
    class DeltaMerge : public core::Operator {
    public:
        explicit DeltaMerge(const std::string &Name, long queryId);

        DeltaMerge(const std::__cxx11::basic_string<char>& tableName,
                   const std::__cxx11::basic_string<char> &Name,
                   long queryId,
                   std::shared_ptr<::arrow::Schema> outputSchema
                   );

        DeltaMerge(std::string name, std::string type, long queryId1,
                   const std::__cxx11::basic_string<char> tableName, const std::basic_string<char> &Name,
                   long queryId);

        DeltaMerge(const std::string& tableName, const std::string &Name, long queryId);

        static std::shared_ptr <DeltaMerge> make(const std::string &Name, long queryId);

        static std::shared_ptr <DeltaMerge> make(const std::string &tableName, const std::string &Name, long queryId,std::shared_ptr<::arrow::Schema> outputSchema );

        void onReceive(const core::message::Envelope &msg) override;

        void onStart();

        void onComplete(const core::message::CompleteMessage &);

        void onTuple(const core::message::TupleMessage &message);


        void addDeltaProducer(const std::shared_ptr<Operator> &deltaProducer);

        void addStableProducer(const std::shared_ptr<Operator> &stableProducer);

    private:
        std::string tableName_;

        std::shared_ptr<::arrow::Schema> outputSchema_;

        int primaryKeyColumnNums_;

        std::unordered_set<std::string> deltaProducerNames_;
        std::unordered_set<std::string> stableProducerNames_;

        std::vector<std::vector<std::shared_ptr<Column>>> deltaTracker_;
        std::vector<std::vector<std::shared_ptr<Column>>> stableTracker_;

        std::vector<int> deltaIndexTracker_;
        std::vector<int> stableIndexTracker_;

        std::vector <std::shared_ptr<TupleSet2>> deltas_;
        std::vector <std::shared_ptr<TupleSet2>> stables_;

//        std::vector<std::array<int,2>> deleteMap_;

        std::unordered_map<int, std::unordered_set<int>> deleteMap_;

        void deltaMerge();

        bool allProducersComplete();

        void populateArrowTrackers();

        bool checkIfAllRecordsWereVisited();

        void generateDeleteMaps();

        std::shared_ptr<TupleSet2> generateFinalResult();
    };

}


#endif //NORMAL_DELTAMERGE_H