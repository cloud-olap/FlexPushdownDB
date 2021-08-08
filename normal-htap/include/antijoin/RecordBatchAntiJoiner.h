//
// Created by Han Cao on 5/20/21.
//

#ifndef NORMAL_RECORDBATCHANTIJOINER_H
#define NORMAL_RECORDBATCHANTIJOINER_H


#include <memory>
#include <utility>

#include <normal/tuple/TupleSetIndexWrapper.h>
#include <normal/tuple/TupleSetIndexFinder.h>
#include <normal/tuple/ArrayAppender.h>
#include <normal/tuple/ColumnName.h>
#include <normal/tuple/TupleSet2.h>
#include <set>
#include "normal/tuple/TupleSetIndexFinderWrapper.h"

using namespace normal::tuple;

namespace normal::pushdown::antijoin {
    class RecordBatchAntiJoiner {
    public:
        RecordBatchAntiJoiner(std::shared_ptr<TupleSetIndex> buildTupleSetIndex,
                              std::string probeJoinColumnName,
                              std::shared_ptr<::arrow::Schema> outputSchema,
                              std::vector<std::shared_ptr<std::pair<bool, int>>> neededColumnIndice);

        static tl::expected <std::shared_ptr<RecordBatchAntiJoiner>, std::string>
        make(const std::shared_ptr<TupleSetIndex> &buildTupleSetIndex,
             const std::string &probeJoinColumnName,
             const std::shared_ptr<::arrow::Schema> &outputSchema,
             const std::vector<std::shared_ptr<std::pair<bool, int>>> &neededColumnIndice);

        tl::expected<void, std::string> antijoin(const std::shared_ptr<::arrow::RecordBatch> &recordBatch);

        tl::expected <std::shared_ptr<TupleSet2>, std::string> toTupleSet();

    private:
        std::shared_ptr<TupleSetIndex> buildTupleSetIndex_;
        std::string probeJoinColumnName_;
        std::shared_ptr<::arrow::Schema> outputSchema_;
        std::vector<std::shared_ptr<std::pair<bool, int>>> neededColumnIndice_;
        std::vector<::arrow::ArrayVector> joinedArrayVectors_;
    };
};


#endif //NORMAL_RECORDBATCHANTIJOINER_H