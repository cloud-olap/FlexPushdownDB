#include "makeTuple.h"

using namespace normal::avro_tuple::make;

LineorderDelta_t MakeTuple::makeLineorderDeltaTuple(i::lineorder& linorderDeltaStruct) {
    return std::make_tuple(
            linorderDeltaStruct.lo_orderkey,
            linorderDeltaStruct.lo_linenumber,
            linorderDeltaStruct.lo_custkey,
            linorderDeltaStruct.lo_partkey,
            linorderDeltaStruct.lo_suppkey,
            linorderDeltaStruct.lo_orderdate,
            linorderDeltaStruct.lo_orderpriority,
            linorderDeltaStruct.lo_shippriority,
            linorderDeltaStruct.lo_quantity,
            linorderDeltaStruct.lo_extendedprice,
            linorderDeltaStruct.lo_ordtotalprice,
            linorderDeltaStruct.lo_discount,
            linorderDeltaStruct.lo_revenue,
            linorderDeltaStruct.lo_supplycost,
            linorderDeltaStruct.lo_tax,
            linorderDeltaStruct.lo_commitdate,
            linorderDeltaStruct.lo_shipmode,
            linorderDeltaStruct.type,
            linorderDeltaStruct.timestamp);
}
