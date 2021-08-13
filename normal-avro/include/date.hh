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


#ifndef __INCLUDE_DATE_HH_2704837145__H_
#define __INCLUDE_DATE_HH_2704837145__H_


#include <sstream>
#include "boost/any.hpp"
#include "avro/Specific.hh"
#include "avro/Encoder.hh"
#include "avro/Decoder.hh"

namespace i {
struct date {
    int32_t d_datekey;
    std::string d_date;
    std::string d_dayofweek;
    std::string d_month;
    int32_t d_year;
    int32_t d_yearmonthnum;
    std::string d_yearmonth;
    int32_t d_daynuminweek;
    int32_t d_daynuminmonth;
    int32_t d_daynuminyear;
    int32_t d_monthnuminyear;
    int32_t d_weeknuminyear;
    std::string d_sellingseason;
    bool d_lastdayinweekfl;
    bool d_lastdayinmonthfl;
    bool d_holidayfl;
    bool d_weekdayfl;
    date() :
        d_datekey(int32_t()),
        d_date(std::string()),
        d_dayofweek(std::string()),
        d_month(std::string()),
        d_year(int32_t()),
        d_yearmonthnum(int32_t()),
        d_yearmonth(std::string()),
        d_daynuminweek(int32_t()),
        d_daynuminmonth(int32_t()),
        d_daynuminyear(int32_t()),
        d_monthnuminyear(int32_t()),
        d_weeknuminyear(int32_t()),
        d_sellingseason(std::string()),
        d_lastdayinweekfl(bool()),
        d_lastdayinmonthfl(bool()),
        d_holidayfl(bool()),
        d_weekdayfl(bool())
        { }
};

}
namespace avro {
template<> struct codec_traits<i::date> {
    static void encode(Encoder& e, const i::date& v) {
        avro::encode(e, v.d_datekey);
        avro::encode(e, v.d_date);
        avro::encode(e, v.d_dayofweek);
        avro::encode(e, v.d_month);
        avro::encode(e, v.d_year);
        avro::encode(e, v.d_yearmonthnum);
        avro::encode(e, v.d_yearmonth);
        avro::encode(e, v.d_daynuminweek);
        avro::encode(e, v.d_daynuminmonth);
        avro::encode(e, v.d_daynuminyear);
        avro::encode(e, v.d_monthnuminyear);
        avro::encode(e, v.d_weeknuminyear);
        avro::encode(e, v.d_sellingseason);
        avro::encode(e, v.d_lastdayinweekfl);
        avro::encode(e, v.d_lastdayinmonthfl);
        avro::encode(e, v.d_holidayfl);
        avro::encode(e, v.d_weekdayfl);
    }
    static void decode(Decoder& d, i::date& v) {
        if (avro::ResolvingDecoder *rd =
            dynamic_cast<avro::ResolvingDecoder *>(&d)) {
            const std::vector<size_t> fo = rd->fieldOrder();
            for (std::vector<size_t>::const_iterator it = fo.begin();
                it != fo.end(); ++it) {
                switch (*it) {
                case 0:
                    avro::decode(d, v.d_datekey);
                    break;
                case 1:
                    avro::decode(d, v.d_date);
                    break;
                case 2:
                    avro::decode(d, v.d_dayofweek);
                    break;
                case 3:
                    avro::decode(d, v.d_month);
                    break;
                case 4:
                    avro::decode(d, v.d_year);
                    break;
                case 5:
                    avro::decode(d, v.d_yearmonthnum);
                    break;
                case 6:
                    avro::decode(d, v.d_yearmonth);
                    break;
                case 7:
                    avro::decode(d, v.d_daynuminweek);
                    break;
                case 8:
                    avro::decode(d, v.d_daynuminmonth);
                    break;
                case 9:
                    avro::decode(d, v.d_daynuminyear);
                    break;
                case 10:
                    avro::decode(d, v.d_monthnuminyear);
                    break;
                case 11:
                    avro::decode(d, v.d_weeknuminyear);
                    break;
                case 12:
                    avro::decode(d, v.d_sellingseason);
                    break;
                case 13:
                    avro::decode(d, v.d_lastdayinweekfl);
                    break;
                case 14:
                    avro::decode(d, v.d_lastdayinmonthfl);
                    break;
                case 15:
                    avro::decode(d, v.d_holidayfl);
                    break;
                case 16:
                    avro::decode(d, v.d_weekdayfl);
                    break;
                default:
                    break;
                }
            }
        } else {
            avro::decode(d, v.d_datekey);
            avro::decode(d, v.d_date);
            avro::decode(d, v.d_dayofweek);
            avro::decode(d, v.d_month);
            avro::decode(d, v.d_year);
            avro::decode(d, v.d_yearmonthnum);
            avro::decode(d, v.d_yearmonth);
            avro::decode(d, v.d_daynuminweek);
            avro::decode(d, v.d_daynuminmonth);
            avro::decode(d, v.d_daynuminyear);
            avro::decode(d, v.d_monthnuminyear);
            avro::decode(d, v.d_weeknuminyear);
            avro::decode(d, v.d_sellingseason);
            avro::decode(d, v.d_lastdayinweekfl);
            avro::decode(d, v.d_lastdayinmonthfl);
            avro::decode(d, v.d_holidayfl);
            avro::decode(d, v.d_weekdayfl);
        }
    }
};

}
#endif
