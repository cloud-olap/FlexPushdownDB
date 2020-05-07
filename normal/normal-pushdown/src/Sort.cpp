//
// Created by Jialing Pei on 5/6/20.
//

#include "normal/pushdown/Sort.h"
namespace normal::pushdown{
    void Sort::onStart() {
        // FIXME: Either set tuples to size 0 or use an optional
        tuples_ = nullptr;
    }

    void Sort::onReceive(const normal::core::message::Envelope &message) {
        if (message.message().type() == "StartMessage") {
            this->onStart();
        } else if (message.message().type() == "TupleMessage") {
            auto tupleMessage = dynamic_cast<const normal::core::message::TupleMessage &>(message.message());
            this->onTuple(tupleMessage);
        } else if (message.message().type() == "CompleteMessage") {
            auto completeMessage = dynamic_cast<const normal::core::message::CompleteMessage &>(message.message());
            this->onComplete(completeMessage);
        } else {
            // FIXME: Propagate error properly
            throw std::runtime_error("Unrecognized message type " + message.message().type());
        }
    }

    void Sort::onTuple(const core::message::TupleMessage &message) {
        SPDLOG_DEBUG("Received tuple message");

        // Set the input schema if not yet set
        cacheInputSchema(message);

        compute(message.tuples());
    }

    void Sort::compute(const std::shared_ptr<normal::core::TupleSet> &tuples){
        SPDLOG_DEBUG("Data:\n{}", tuples->toString());

        // Set the input schema if not yet set

        // Build and set the expression projector if not yet set
        buildAndCacheProjector();

        auto resultType = this->expression_->getReturnType();
        std::shared_ptr<arrow::Scalar> batchSum = tuples->visit([&](auto accum, auto &batch) -> auto {

            auto arrayVector = projector_.value()->evaluate(batch);
            auto array = arrayVector->at(0);

            return accum;
        });
    }
    void Sort::buildAndCacheProjector() {
        if(!projector_.has_value()){
            auto expressionsVec = {this->expression_};
            projector_ = std::make_shared<expression::gandiva::Projector>(expressionsVec);
            projector_.value()->compile(inputSchema_.value());
        }
    }

    void Sort::cacheInputSchema(const core::message::TupleMessage &message) {
        if(!inputSchema_.has_value()){
            inputSchema_ = message.tuples()->table()->schema();
        }
    }


}
