//
// Created by Han Cao on 7/15/21.
//

#ifndef NORMAL_DELTAMERGE_H
#define NORMAL_DELTAMERGE_H

#include <normal/core/Operator.h>

#include <normal/core/message/CompleteMessage.h>
#include <normal/pushdown/TupleMessage.h>

namespace normal::pushdown::deltamerge {
    class DeltaMerge : public Operator {
    public:
        explicit DeltaMerge(const std::string &Name, long queryId);

        static std::shared_ptr <DeltaMerge> make(const std::string &Name, long queryId);

        void onReceive(const Envelope &msg) override;

        void onStart();

        void onComplete(const COmpleteMessage &);

        void onTuple(const TupleMessage &message);


    private:
        void DeltaMerge();

        std::weak_ptr <std::unordered_map<std::string>, Operator> deltaProducers_;
        std::weak_ptr <std::unordered_map<std::string>, Operator> stableProducers_;

        std::list <std::shared_ptr<TupleSet2>> deltas_;
        std::list <std::shared_ptr<TupleSet2>> stables_;
    }

}


#endif //NORMAL_DELTAMERGE_H