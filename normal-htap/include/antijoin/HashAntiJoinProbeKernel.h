//
// Created by Han Cao on 5/19/21.
//

#ifndef NORMAL_HASHANTIJOINPROBEKERNEL_H
#define NORMAL_HASHANTIJOINPROBEKERNEL_H

#include <memory>

#include "normal/pushdown/join/JoinPredicate.h"
#include "normal/pushdown/join/ATTIC/HashTable.h"
#include "normal/tuple/TupleSetIndex.h"
//#include "RecordBatchJoiner.h"
#include <set>

#include <normal/tuple/TupleSet2.h>

using namespace normal::tuple;

namespace normal::pushdown::antijoin {

    class HashAntiJoinProbeKernel {
    public:
        explicit HashAntiJoinProbeKernel(join::JoinPredicate pred, std::set<std::string> neededColumnNames);

        static HashAntiJoinProbeKernel make(join::JoinPredicate pred, std::set<std::string> neededColumnNames);

        tl::expected<void, std::string> joinBuildTupleSetIndex(const std::shared_ptr<TupleSetIndex> &tupleSetIndex);

        tl::expected<void, std::string> joinProbeTupleSet(const std::shared_ptr<TupleSet2> &tupleSet);

        const std::optional<std::shared_ptr<normal::tuple::TupleSet2>> &getBuffer() const;

        void clear();

        [[nodiscard]] tl::expected<void, std::string>
        putBuildTupleSetIndex(const std::shared_ptr<TupleSetIndex> &tupleSetIndex);

        [[nodiscard]] tl::expected<void, std::string> putProbeTupleSet(const std::shared_ptr<TupleSet2> &tupleSet);

    private:
        join::JoinPredicate pred_;
        std::optional<std::shared_ptr<TupleSetIndex>> buildTupleSetIndex_;
        std::optional<std::shared_ptr<TupleSet2>> probeTupleSet_;
        std::set<std::string> neededColumnNames_;
        std::optional<std::shared_ptr<::arrow::Schema>> outputSchema_;
        std::vector<std::shared_ptr<std::pair<bool, int>>> neededColumnIndice_;   // <true/false, i> -> ith column in build/probe table
        std::optional<std::shared_ptr<normal::tuple::TupleSet2>> buffer_;

        [[nodiscard]] tl::expected<void, std::string> buffer(const std::shared_ptr<TupleSet2> &tupleSet);

        void bufferOutputSchema(const std::shared_ptr<TupleSetIndex> &tupleSetIndex,
                                const std::shared_ptr<TupleSet2> &tupleSet);
    };
}


#endif //NORMAL_HASHANTIJOINPROBEKERNEL_H
