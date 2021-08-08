//
// Created by ZhangOscar on 8/8/21.
//

#ifndef NORMAL_DELTAPUMP_H
#define NORMAL_DELTAPUMP_H

#include <normal/core/Operator.h>

#include <normal/core/message/Message.h>
#include <normal/core/message/CompleteMessage.h>
#include <normal/core/message/TupleMessage.h>

namespace normal::htap::deltapump {

    class DeltaPump : public normal::core::Operator {
    public:
        explicit DeltaPump(const std::string &binlog_path);

        static std::shared_ptr <DeltaPump> make(const std::string &binlog_path);

        void onReceive(const normal::core::message::Envelope &message) override;

        void onTuple(const normal::core::message::TupleMessage &message);

        void onComplete(const normal::core::message::CompleteMessage &message);

        void onStart();
    };
}

#endif //NORMAL_DELTAPUMP_H
