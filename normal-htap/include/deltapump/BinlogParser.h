//
// Created by ZhangOscar on 8/8/21.
//

#ifndef NORMAL_BINLOGPARSER_H
#define NORMAL_BINLOGPARSER_H

#include <jni.h>
#include <cassert>
#include <iostream>
#include <vector>
#include <string>
#include <cstdio>
#include <ctime>
#include <cmath> //floor
#include <unordered_map> // std::unordered_map
#include <utility> // std::pair
#include <tuple> // std::tuple
#include <iterator>
#include <set> //std::set
#include <stdexcept> // std::runtime_error
#include <sstream> // std::stringstream

#include <fstream>
#include <complex>

#include "lineorder_d.hh"
#include "customer_d.hh"
#include "supplier_d.hh"
#include "part_d.hh"
#include "date_d.hh"
#include "avro/Encoder.hh"
#include "avro/Decoder.hh"
#include "avro/ValidSchema.hh"
#include "avro/Compiler.hh"
#include "avro/DataFile.hh"

#include "avro/Stream.hh"
#include "avro/Specific.hh"
#include "avro/Generic.hh"

#include "makeTuple.h"

using namespace normal::avro_tuple::make;

struct lineorder_record{
    int orderkey;
    int linenumber;
    LineorderDelta_t lineorder_delta;

    bool operator<(const lineorder_record& l) const
    {
        return ((this->orderkey < l.orderkey) || (this->orderkey == l.orderkey && this->linenumber < l.linenumber));
    }
};

/*
 * load avro schema from disk
 */

avro::ValidSchema loadSchema(const char* filename);

/*
 * function to call functions in java and receive serialized avro data returned from java side
 * return pointers of partitioned tables
 */
void parse(const char *filePath,  const char *rangeFilePath, std::unordered_map<int, std::set<struct lineorder_record>> **lineorder_record_ptr);



#endif //NORMAL_BINLOGPARSER_H
