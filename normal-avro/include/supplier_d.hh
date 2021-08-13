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


#ifndef __INCLUDE_SUPPLIER_D_HH_3481302679__H_
#define __INCLUDE_SUPPLIER_D_HH_3481302679__H_


#include <sstream>
#include "boost/any.hpp"
#include "avro/Specific.hh"
#include "avro/Encoder.hh"
#include "avro/Decoder.hh"

namespace i {
struct supplier {
    int32_t s_suppkey;
    std::string s_name;
    std::string s_address;
    std::string s_city;
    std::string s_nation;
    std::string s_region;
    std::string s_phone;
    std::string type;
    int32_t timestamp;
    supplier() :
        s_suppkey(int32_t()),
        s_name(std::string()),
        s_address(std::string()),
        s_city(std::string()),
        s_nation(std::string()),
        s_region(std::string()),
        s_phone(std::string()),
        type(std::string()),
        timestamp(int32_t())
        { }
};

}
namespace avro {
template<> struct codec_traits<i::supplier> {
    static void encode(Encoder& e, const i::supplier& v) {
        avro::encode(e, v.s_suppkey);
        avro::encode(e, v.s_name);
        avro::encode(e, v.s_address);
        avro::encode(e, v.s_city);
        avro::encode(e, v.s_nation);
        avro::encode(e, v.s_region);
        avro::encode(e, v.s_phone);
        avro::encode(e, v.type);
        avro::encode(e, v.timestamp);
    }
    static void decode(Decoder& d, i::supplier& v) {
        if (avro::ResolvingDecoder *rd =
            dynamic_cast<avro::ResolvingDecoder *>(&d)) {
            const std::vector<size_t> fo = rd->fieldOrder();
            for (std::vector<size_t>::const_iterator it = fo.begin();
                it != fo.end(); ++it) {
                switch (*it) {
                case 0:
                    avro::decode(d, v.s_suppkey);
                    break;
                case 1:
                    avro::decode(d, v.s_name);
                    break;
                case 2:
                    avro::decode(d, v.s_address);
                    break;
                case 3:
                    avro::decode(d, v.s_city);
                    break;
                case 4:
                    avro::decode(d, v.s_nation);
                    break;
                case 5:
                    avro::decode(d, v.s_region);
                    break;
                case 6:
                    avro::decode(d, v.s_phone);
                    break;
                case 7:
                    avro::decode(d, v.type);
                    break;
                case 8:
                    avro::decode(d, v.timestamp);
                    break;
                default:
                    break;
                }
            }
        } else {
            avro::decode(d, v.s_suppkey);
            avro::decode(d, v.s_name);
            avro::decode(d, v.s_address);
            avro::decode(d, v.s_city);
            avro::decode(d, v.s_nation);
            avro::decode(d, v.s_region);
            avro::decode(d, v.s_phone);
            avro::decode(d, v.type);
            avro::decode(d, v.timestamp);
        }
    }
};

}
#endif
