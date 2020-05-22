//
// Created by Jialing Pei on 5/6/20.
//

#include <normal/pushdown/Sort.h>
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

    void Sort::onComplete(const normal::core::message::CompleteMessage &message) {
        //this is where the real sort happens
        auto pri = priorities_;
        std::vector<int> idx = std::vector<int>(tmpRes_->size());
        iota(idx.begin(),idx.end(),0);
        std::stable_sort(idx.begin(),idx.end(),Comparator(pri,tmpRes_));
        //sort tuples according to idx
        arrow::MemoryPool* pool = arrow::default_memory_pool();
        auto allColumns = tuples_->table()->columns();
        auto allFields = tuples_->table()->fields();
        std::vector<std::shared_ptr<arrow::Array>> newArrList;
        for (auto i=0; i< allColumns.size();++i){
            auto column = allColumns.at(i);
            auto colType = column->type();
            auto field =  allFields.at(i);
            //Todo: add more cases
            if (colType->Equals(arrow::Int64Type())) {
                    auto typedColumn = std::static_pointer_cast<arrow::Int64Array>(column->chunk(0));
                    arrow::Int64Builder builder(pool);
                    for (auto it:idx){
                        builder.Append(typedColumn->Value(it));
                        SPDLOG_INFO(typedColumn->Value(it));
                    }
                    std::shared_ptr<arrow::Array> newArray;
                    builder.Finish(&newArray);
                    newArrList.push_back(newArray);
                    SPDLOG_INFO(newArray->ToString());
            } else if (colType->Equals(arrow::Int32Type())) {
                auto typedColumn = std::static_pointer_cast<arrow::Int32Array>(column->chunk(0));
                arrow::Int32Builder builder(pool);
                for (auto it:idx){
                    builder.Append(typedColumn->Value(it));
                }
                std::shared_ptr<arrow::Array> newArray;
                builder.Finish(&newArray);
                newArrList.push_back(newArray);
            }

        }
        auto newTable = arrow::Table::Make(tuples_->table()->schema(),newArrList);
        //tuples_->table() = newTable;
        //SPDLOG_INFO(tuples_->table()->column(0)->chunk(0)->ToString());
        auto newTuples  = TupleSet::make(newTable);
        std::shared_ptr<normal::core::message::Message> tpmessage = std::make_shared<normal::core::message::TupleMessage>(newTuples, this->name());
        ctx()->tell(tpmessage);
        ctx()->notifyComplete();
    }
    void Sort::onTuple(const core::message::TupleMessage &message) {
        SPDLOG_DEBUG("Received tuple message");

        // Set the input schema if not yet set
        cacheInputSchema(message);

        compute(message.tuples());
    }

    void Sort::compute(const std::shared_ptr<TupleSet> &tuples){
        SPDLOG_DEBUG("Data:\n{}", tuples->toString());

        //concantenate tuples
        if (tuples_== nullptr) {
            tuples_ = tuples;
        } else {
            tuples_ = TupleSet::concatenate(tuples, tuples_);
        }
        // Set the input schema if not yet set

        // Build and set the expression projector if not yet set
        buildAndCacheProjector();




        auto resultType = this->expression_->getReturnType();
        std::shared_ptr<arrow::Scalar> batchSum = tuples->visit([&](auto accum, auto &batch) -> auto {

            // Initialise accumulator
            if(accum == nullptr) {
                if (resultType->id() == arrow::float64()->id()) {
                    accum = arrow::MakeScalar(arrow::float64(), 0.0).ValueOrDie();
                } else if (resultType->id() == arrow::int32()->id()) {
                    accum = arrow::MakeScalar(arrow::int32(), 0).ValueOrDie();
                } else if (resultType->id() == arrow::int64()->id()) {
                    accum = arrow::MakeScalar(arrow::int64(), 0).ValueOrDie();
                } else {
                    throw std::runtime_error("Accumulator init for type " + accum->type->name() + " not implemented yet");
                }
            }


            auto arrayVector = projector_.value()->evaluate(batch);
            auto array = arrayVector->at(0);
            int numOfFields = arrayVector->size();
            int lenOfBatch = array->length();
            for (int i=0; i<lenOfBatch; ++i){
                std::vector<Cell> row;
                for (int j=0; j<numOfFields; ++j){
                    array = arrayVector->at(j);
                    auto colType = array->type();
                    Cell cell;
                    if (colType->Equals(arrow::Int32Type())) {
                        auto typedArray = std::static_pointer_cast<arrow::Int32Array>(array);
                        cell.type = Cell::is_int;
                        cell.val.ival = typedArray->Value(i);
                    }
                    else if (colType->Equals(arrow::Int64Type())) {
                        auto typedArray = std::static_pointer_cast<arrow::Int64Array>(array);
                        cell.type = Cell::is_int;
                        cell.val.ival = typedArray->Value(i);
                    }
                    else if (colType->Equals(arrow::DoubleType())) {
                        auto typedArray = std::static_pointer_cast<arrow::DoubleArray>(array);
                        cell.type = Cell::is_float;
                        cell.val.fval = typedArray->Value(i);
                    }
                    else if (colType->Equals(arrow::StringType())) {
                        auto typedArray = std::static_pointer_cast<arrow::StringArray>(array);
                        cell.type = Cell::is_char;
                        //cell.val.cval = typedArray->Value(i);
                    }
                    row.push_back(cell);
                }
                tmpRes_->push_back(row);
            }
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
