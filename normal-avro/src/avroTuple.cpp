//
// Created by Han Cao on 8/15/21.
//

#include "normal/avro_tuple/avroTuple.h"

using namespace normal::avro_tuple;

avroTuple::avroTuple(bool isDelta, std::string table_name) : tableName_(table_name),  isDelta_(isDelta) {
}

std::shared_ptr<avroTuple> avroTuple::make(bool isDelta, std::string schema_name) {
    return std::make_shared<avroTuple>(avroTuple(isDelta, schema_name));
}

avroTuple::avroTuple(std::vector<avro::GenericRecord> v, std::string table_name, bool is_delta) {
    avroVector_ = v;
    tableName_ = table_name;
    isDelta_ = is_delta;
}

std::shared_ptr<avroTuple> make(std::vector<avro::GenericRecord> v, std::string table_name, bool is_delta) {
    return std::make_shared<avroTuple>(avroTuple(v, table_name, is_delta));
}

