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


#ifndef __INCLUDE_PART_HH_3685383467__H_
#define __INCLUDE_PART_HH_3685383467__H_


#include <sstream>
#include "boost/any.hpp"
#include "avro/Specific.hh"
#include "avro/Encoder.hh"
#include "avro/Decoder.hh"

namespace i {
struct part {
    int32_t p_partkey;
    std::string p_name;
    std::string p_mfgr;
    std::string p_category;
    std::string p_brand1;
    std::string p_color;
    std::string p_type;
    int32_t p_size;
    std::string p_container;
    part() :
        p_partkey(int32_t()),
        p_name(std::string()),
        p_mfgr(std::string()),
        p_category(std::string()),
        p_brand1(std::string()),
        p_color(std::string()),
        p_type(std::string()),
        p_size(int32_t()),
        p_container(std::string())
        { }
};

}
namespace avro {
template<> struct codec_traits<i::part> {
    static void encode(Encoder& e, const i::part& v) {
        avro::encode(e, v.p_partkey);
        avro::encode(e, v.p_name);
        avro::encode(e, v.p_mfgr);
        avro::encode(e, v.p_category);
        avro::encode(e, v.p_brand1);
        avro::encode(e, v.p_color);
        avro::encode(e, v.p_type);
        avro::encode(e, v.p_size);
        avro::encode(e, v.p_container);
    }
    static void decode(Decoder& d, i::part& v) {
        if (avro::ResolvingDecoder *rd =
            dynamic_cast<avro::ResolvingDecoder *>(&d)) {
            const std::vector<size_t> fo = rd->fieldOrder();
            for (std::vector<size_t>::const_iterator it = fo.begin();
                it != fo.end(); ++it) {
                switch (*it) {
                case 0:
                    avro::decode(d, v.p_partkey);
                    break;
                case 1:
                    avro::decode(d, v.p_name);
                    break;
                case 2:
                    avro::decode(d, v.p_mfgr);
                    break;
                case 3:
                    avro::decode(d, v.p_category);
                    break;
                case 4:
                    avro::decode(d, v.p_brand1);
                    break;
                case 5:
                    avro::decode(d, v.p_color);
                    break;
                case 6:
                    avro::decode(d, v.p_type);
                    break;
                case 7:
                    avro::decode(d, v.p_size);
                    break;
                case 8:
                    avro::decode(d, v.p_container);
                    break;
                default:
                    break;
                }
            }
        } else {
            avro::decode(d, v.p_partkey);
            avro::decode(d, v.p_name);
            avro::decode(d, v.p_mfgr);
            avro::decode(d, v.p_category);
            avro::decode(d, v.p_brand1);
            avro::decode(d, v.p_color);
            avro::decode(d, v.p_type);
            avro::decode(d, v.p_size);
            avro::decode(d, v.p_container);
        }
    }
};

}
#endif
