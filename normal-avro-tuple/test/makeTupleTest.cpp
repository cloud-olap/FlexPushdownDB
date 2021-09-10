//
// Created by ZhangOscar on 9/9/21.
//

#include <normal/avro_tuple/makeTuple.h>

#include "normal/avro_tuple/lineorder_d.hh"

using namespace normal::avro_tuple::make;

int main(void) {
    i::lineorder test_data;
    test_data.lo_orderkey = 30189312;
    test_data.lo_linenumber = 6;
    test_data.lo_custkey = 9999;
    test_data.lo_shippriority = "FOB";
    test_data.type = "WRITE";
    test_data.timestamp = 123456;

    LineorderDelta_t delta = MakeTuple::makeLineorderDeltaTuple(test_data);
    auto x = std::get<0>(delta); // call by index on tuple
    std::cout << x << std::endl;
    return 0;
}
