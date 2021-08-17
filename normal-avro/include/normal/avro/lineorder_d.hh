/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


#ifndef __INCLUDE_LINEORDER_D_HH_3481302679__H_
#define __INCLUDE_LINEORDER_D_HH_3481302679__H_


#include <sstream>
#include "boost/any.hpp"
#include "avro/Specific.hh"
#include "avro/Encoder.hh"
#include "avro/Decoder.hh"

namespace i {
struct date {
    int32_t lo_orderkey;
    int32_t lo_linenumber;
    int32_t lo_custkey;
    int32_t lo_partkey;
    int32_t lo_suppkey;
    int32_t lo_orderdate;
    std::string lo_orderpriority;
    std::string lo_shippriority;
    int32_t lo_quantity;
    int32_t lo_extendedprice;
    int32_t lo_ordtotalprice;
    int32_t lo_discount;
    int64_t lo_revenue;
    int64_t lo_supplycost;
    int32_t lo_tax;
    int32_t lo_commitdate;
    std::string lo_shipmode;
    std::string type;
    int32_t timestamp;
    date() :
        lo_orderkey(int32_t()),
        lo_linenumber(int32_t()),
        lo_custkey(int32_t()),
        lo_partkey(int32_t()),
        lo_suppkey(int32_t()),
        lo_orderdate(int32_t()),
        lo_orderpriority(std::string()),
        lo_shippriority(std::string()),
        lo_quantity(int32_t()),
        lo_extendedprice(int32_t()),
        lo_ordtotalprice(int32_t()),
        lo_discount(int32_t()),
        lo_revenue(int64_t()),
        lo_supplycost(int64_t()),
        lo_tax(int32_t()),
        lo_commitdate(int32_t()),
        lo_shipmode(std::string()),
        type(std::string()),
        timestamp(int32_t())
        { }
};

}
namespace avro {
template<> struct codec_traits<i::date> {
    static void encode(Encoder& e, const i::date& v) {
        avro::encode(e, v.lo_orderkey);
        avro::encode(e, v.lo_linenumber);
        avro::encode(e, v.lo_custkey);
        avro::encode(e, v.lo_partkey);
        avro::encode(e, v.lo_suppkey);
        avro::encode(e, v.lo_orderdate);
        avro::encode(e, v.lo_orderpriority);
        avro::encode(e, v.lo_shippriority);
        avro::encode(e, v.lo_quantity);
        avro::encode(e, v.lo_extendedprice);
        avro::encode(e, v.lo_ordtotalprice);
        avro::encode(e, v.lo_discount);
        avro::encode(e, v.lo_revenue);
        avro::encode(e, v.lo_supplycost);
        avro::encode(e, v.lo_tax);
        avro::encode(e, v.lo_commitdate);
        avro::encode(e, v.lo_shipmode);
        avro::encode(e, v.type);
        avro::encode(e, v.timestamp);
    }
    static void decode(Decoder& d, i::date& v) {
        if (avro::ResolvingDecoder *rd =
            dynamic_cast<avro::ResolvingDecoder *>(&d)) {
            const std::vector<size_t> fo = rd->fieldOrder();
            for (std::vector<size_t>::const_iterator it = fo.begin();
                it != fo.end(); ++it) {
                switch (*it) {
                case 0:
                    avro::decode(d, v.lo_orderkey);
                    break;
                case 1:
                    avro::decode(d, v.lo_linenumber);
                    break;
                case 2:
                    avro::decode(d, v.lo_custkey);
                    break;
                case 3:
                    avro::decode(d, v.lo_partkey);
                    break;
                case 4:
                    avro::decode(d, v.lo_suppkey);
                    break;
                case 5:
                    avro::decode(d, v.lo_orderdate);
                    break;
                case 6:
                    avro::decode(d, v.lo_orderpriority);
                    break;
                case 7:
                    avro::decode(d, v.lo_shippriority);
                    break;
                case 8:
                    avro::decode(d, v.lo_quantity);
                    break;
                case 9:
                    avro::decode(d, v.lo_extendedprice);
                    break;
                case 10:
                    avro::decode(d, v.lo_ordtotalprice);
                    break;
                case 11:
                    avro::decode(d, v.lo_discount);
                    break;
                case 12:
                    avro::decode(d, v.lo_revenue);
                    break;
                case 13:
                    avro::decode(d, v.lo_supplycost);
                    break;
                case 14:
                    avro::decode(d, v.lo_tax);
                    break;
                case 15:
                    avro::decode(d, v.lo_commitdate);
                    break;
                case 16:
                    avro::decode(d, v.lo_shipmode);
                    break;
                case 17:
                    avro::decode(d, v.type);
                    break;
                case 18:
                    avro::decode(d, v.timestamp);
                    break;
                default:
                    break;
                }
            }
        } else {
            avro::decode(d, v.lo_orderkey);
            avro::decode(d, v.lo_linenumber);
            avro::decode(d, v.lo_custkey);
            avro::decode(d, v.lo_partkey);
            avro::decode(d, v.lo_suppkey);
            avro::decode(d, v.lo_orderdate);
            avro::decode(d, v.lo_orderpriority);
            avro::decode(d, v.lo_shippriority);
            avro::decode(d, v.lo_quantity);
            avro::decode(d, v.lo_extendedprice);
            avro::decode(d, v.lo_ordtotalprice);
            avro::decode(d, v.lo_discount);
            avro::decode(d, v.lo_revenue);
            avro::decode(d, v.lo_supplycost);
            avro::decode(d, v.lo_tax);
            avro::decode(d, v.lo_commitdate);
            avro::decode(d, v.lo_shipmode);
            avro::decode(d, v.type);
            avro::decode(d, v.timestamp);
        }
    }
};

}
#endif
