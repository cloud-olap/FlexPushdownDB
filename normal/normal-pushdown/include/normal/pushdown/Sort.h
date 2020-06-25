//
// Created by Jialing Pei on 5/6/20.
//

#include <normal/core/Operator.h>
#include <normal/tuple/Schema.h>
#include <normal/pushdown/TupleMessage.h>
#include <normal/core/message/CompleteMessage.h>
#include <normal/expression/gandiva/Projector.h>

#ifndef NORMAL_SORT_H
#define NORMAL_SORT_H


namespace normal::pushdown {
    struct Cell{
        Cell():type(is_int),val(){};
        enum { is_int, is_float, is_char } type;
        union {
            int ival;
            float fval;
            char cval;
        } val;
    };

    struct Comparator{
        Comparator(std::shared_ptr<std::vector<int>> priorities,std::shared_ptr<std::vector<std::vector<Cell>>> vectors): priorities_(priorities),
        vectors_(vectors){}
        bool operator() (int  idx1, int  idx2) {
            auto a = vectors_->at(idx1);
            auto b = vectors_->at(idx2);
            for (size_t i=0; i<priorities_->size(); ++i){
                int index = priorities_->at(i);
                if (a.at(index).type==Cell::is_int){
                    return a.at(index).val.ival < b.at(index).val.ival;
                }
                else if (a.at(index).type==Cell::is_float){
                    return a.at(index).val.fval < b.at(index).val.fval;
                }
                else if (a.at(index).type==Cell::is_char){
                    return a.at(index).val.cval < b.at(index).val.cval;
                }
            }
            return true;
        }
        std::shared_ptr<std::vector<int>> priorities_;
        std::shared_ptr<std::vector<std::vector<Cell>>> vectors_;
    };
    class Sort : public normal::core::Operator {

    private:
        /**
        * A buffer of received tuples that stores the temporal result
        */
        std::shared_ptr<TupleSet> tuples_;
        std::shared_ptr<std::vector<std::vector<Cell>>> tmpRes_;
        std::shared_ptr<std::vector<int>> priorities_;
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
        bool compareTwoTuples(std::vector<Cell> & a,std::vector<Cell> & b);

    public:
        Sort(std::string name,std::shared_ptr<normal::expression::gandiva::Expression> expression,std::shared_ptr<std::vector<int>> priorities):
        Operator(std::move(name), "Sort"),
		tmpRes_(std::make_shared<std::vector<std::vector<Cell>>>(std::vector<std::vector<Cell>>{})),
		priorities_(std::move(priorities)),
		expression_(std::move(expression)){};
        ~Sort() override = default;

        void compute(const std::shared_ptr<TupleSet> &tuples);
        void cacheInputSchema(const core::message::TupleMessage &message);
        void buildAndCacheProjector();


    };

}


#endif //NORMAL_SORT_H
