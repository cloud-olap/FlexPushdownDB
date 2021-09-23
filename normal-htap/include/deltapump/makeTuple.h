#ifndef NORMAL_MAKETUPLE_H
#define NORMAL_MAKETUPLE_H

#include <tuple>
#include <string>

#include "lineorder_d.hh"

namespace normal::avro_tuple::make {

typedef std::tuple<
    int32_t,
    int32_t,
    int32_t,
    int32_t,
    int32_t,
    int32_t,
    std::string,
    std::string,
    int32_t,
    int32_t,
    int32_t,
    int32_t,
    int64_t,
    int64_t,
    int32_t,
    int32_t,
    std::string,
    std::string,
    int32_t> LineorderDelta_t;

class MakeTuple {
public:
    static LineorderDelta_t makeLineorderDeltaTuple(i::lineorder& linorderDeltaStruct);

};

}

#endif //NORMAL_MAKETUPLE_H
