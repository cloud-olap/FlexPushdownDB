//
// Created by Jialing Pei on 5/6/20.
//

#include <normal/core/Operator.h>
#include <normal/tuple/Schema.h>
#include <normal/core/message/TupleMessage.h>
#include <normal/core/message/CompleteMessage.h>
#include <normal/expression/gandiva/Projector.h>

#ifndef NORMAL_SORT_H
#define NORMAL_SORT_H


namespace normal::pushdown {
    class Sort : public normal::core::Operator {

    private:
        /**
        * A buffer of received tuples that stores the temporal result
        */
        std::shared_ptr<normal::core::TupleSet> tuples_;

        /**
         * The schema of received tuples, sometimes cannot be known up front (e.g. when input source is a CSV file, the
         * columns aren't known until the file is read) so needs to be extracted from the first batch of tuples received
         */
        std::optional<std::shared_ptr<arrow::Schema>> inputSchema_;

        /**
         *
         * sort columns names
         */
        std::shared_ptr<std::vector<std::shared_ptr<std::string>>> sortColumns_;
        std::shared_ptr<normal::expression::gandiva::Expression> expression_;

        /**
        * The expression projector, created and cached when input schema is extracted from first tuple received
        */
        std::optional<std::shared_ptr<normal::expression::Projector>> projector_;

        void onReceive(const normal::core::message::Envelope &message) override;

        void onTuple(const normal::core::message::TupleMessage &message);
        void onComplete(const normal::core::message::CompleteMessage &message);
        void onStart();

    public:
        Sort(std::string name,std::shared_ptr<normal::expression::gandiva::Expression> expression);
        ~Sort() override = default;

        void compute(const std::shared_ptr<normal::core::TupleSet> &tuples);
        void cacheInputSchema(const core::message::TupleMessage &message);
        void buildAndCacheProjector();

    };

}


#endif //NORMAL_SORT_H
