//
// Created by Han Cao on 7/15/21.
//

#ifndef NORMAL_DELTAMERGE_H
#define NORMAL_DELTAMERGE_H

#include <normal/core/Operator.h>

#include <normal/core/message/CompleteMessage.h>
#include <normal/core/message/TupleMessage.h>
#include <normal/tuple/TupleSet2.h>

namespace normal::pushdown::deltamerge {
    class DeltaMerge : public core::Operator {
    public:
        explicit DeltaMerge(const std::string &Name, long queryId);

        static std::shared_ptr <DeltaMerge> make(const std::string &Name, long queryId);

        void onReceive(const core::message::Envelope &msg) override;

        void onStart();

        void onComplete(const core::message::CompleteMessage &);

        void onTuple(const core::message::TupleMessage &message);


    private:
        std::unordered_map<std::string, std::weak_ptr<Operator>> deltaProducers_;
        std::unordered_map<std::string, std::weak_ptr<Operator>> stableProducers_;

        std::list <std::shared_ptr<TupleSet2>> deltas_;
        std::list <std::shared_ptr<TupleSet2>> stables_;

        void deltaMerge();

        void addStableProducer(const std::shared_ptr<Operator> &stableProducer);

        void addDeltaProducer(const std::shared_ptr<Operator> &deltaProducer);

        bool allProducersComplete();
    }

}


#endif //NORMAL_DELTAMERGE_H