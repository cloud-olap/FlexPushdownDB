//
// Created by Han Cao on 5/19/21.
//

#ifndef NORMAL_HASHANTIJOINPROBE_H
#define NORMAL_HASHANTIJOINPROBE_H

#include <normal/core/Operator.h>
#include <normal/pushdown/TupleMessage.h>
#include <normal/core/message/CompleteMessage.h>

#include "normal/pushdown/join/JoinPredicate.h"
#include "normal/pushdown/join/ATTIC/HashTableMessage.h"
#include "normal/pushdown/join/ATTIC/HashTable.h"
#include "normal/pushdown/join/ATTIC/HashJoinProbeKernel.h"
#include <normal/pushdown/join/TupleSetIndexMessage.h>

#include "HashAntiJoinProbeKernel.h"

namespace normal::pushdown::antijoin {
    class HashAntiJoinProbe : public normal::core::Operator {

    public:
        HashAntiJoinProbe(const std::string &name, join::JoinPredicate pred, std::set<std::string> neededColumnNames,
                          long queryId = 0);

        static HashAntiJoinProbe
        make(const std::string &name, join::JoinPredicate pred, std::set<std::string> neededColumnNames,
             long queryId = 0);

        static std::shared_ptr<HashAntiJoinProbe>
        create(const std::string &name, join::JoinPredicate pred, std::set<std::string> neededColumnNames,
               long queryId = 0);

        void onReceive(const core::message::Envelope &msg) override;

    private:

        HashAntiJoinProbeKernel kernel_;

        void onStart();

        void onTuple(const core::message::TupleMessage &msg);

        void onHashTable(const join::TupleSetIndexMessage &msg);

        void onComplete(const core::message::CompleteMessage &msg);

        void send(bool force);
    };
}

#endif //NORMAL_HASHANTIJOINPROBE_H
