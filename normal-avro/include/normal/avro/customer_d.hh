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


#ifndef __INCLUDE_CUSTOMER_D_HH_3481302679__H_
#define __INCLUDE_CUSTOMER_D_HH_3481302679__H_


#include <sstream>
#include "boost/any.hpp"
#include "avro/Specific.hh"
#include "avro/Encoder.hh"
#include "avro/Decoder.hh"

namespace i {
struct customer {
    int32_t c_custkey;
    std::string c_name;
    std::string c_address;
    std::string c_city;
    std::string c_nation;
    std::string c_region;
    std::string c_phone;
    std::string c_mktsegment;
    std::string type;
    int32_t timestamp;
    customer() :
        c_custkey(int32_t()),
        c_name(std::string()),
        c_address(std::string()),
        c_city(std::string()),
        c_nation(std::string()),
        c_region(std::string()),
        c_phone(std::string()),
        c_mktsegment(std::string()),
        type(std::string()),
        timestamp(int32_t())
        { }
};

}
namespace avro {
template<> struct codec_traits<i::customer> {
    static void encode(Encoder& e, const i::customer& v) {
        avro::encode(e, v.c_custkey);
        avro::encode(e, v.c_name);
        avro::encode(e, v.c_address);
        avro::encode(e, v.c_city);
        avro::encode(e, v.c_nation);
        avro::encode(e, v.c_region);
        avro::encode(e, v.c_phone);
        avro::encode(e, v.c_mktsegment);
        avro::encode(e, v.type);
        avro::encode(e, v.timestamp);
    }
    static void decode(Decoder& d, i::customer& v) {
        if (avro::ResolvingDecoder *rd =
            dynamic_cast<avro::ResolvingDecoder *>(&d)) {
            const std::vector<size_t> fo = rd->fieldOrder();
            for (std::vector<size_t>::const_iterator it = fo.begin();
                it != fo.end(); ++it) {
                switch (*it) {
                case 0:
                    avro::decode(d, v.c_custkey);
                    break;
                case 1:
                    avro::decode(d, v.c_name);
                    break;
                case 2:
                    avro::decode(d, v.c_address);
                    break;
                case 3:
                    avro::decode(d, v.c_city);
                    break;
                case 4:
                    avro::decode(d, v.c_nation);
                    break;
                case 5:
                    avro::decode(d, v.c_region);
                    break;
                case 6:
                    avro::decode(d, v.c_phone);
                    break;
                case 7:
                    avro::decode(d, v.c_mktsegment);
                    break;
                case 8:
                    avro::decode(d, v.type);
                    break;
                case 9:
                    avro::decode(d, v.timestamp);
                    break;
                default:
                    break;
                }
            }
        } else {
            avro::decode(d, v.c_custkey);
            avro::decode(d, v.c_name);
            avro::decode(d, v.c_address);
            avro::decode(d, v.c_city);
            avro::decode(d, v.c_nation);
            avro::decode(d, v.c_region);
            avro::decode(d, v.c_phone);
            avro::decode(d, v.c_mktsegment);
            avro::decode(d, v.type);
            avro::decode(d, v.timestamp);
        }
    }
};

}
#endif
