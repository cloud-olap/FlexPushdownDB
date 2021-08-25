//
// Created by Han Cao on 8/15/21.
//

#include "normal/avro_tuple/AvroTuple.h"

using namespace normal::avro_tuple;

AvroTuple::AvroTuple(bool isDelta, std::string table_name) : tableName_(table_name),  isDelta_(isDelta) {
}

AvroTuple::AvroTuple(std::vector<avro::GenericRecord> v, std::string table_name, bool is_delta) {
    avroVector_ = v;
    tableName_ = table_name;
    isDelta_ = is_delta;
}

std::shared_ptr<AvroTuple> AvroTuple::make(bool isDelta, std::string schema_name) {
    return std::make_shared<AvroTuple>(AvroTuple(isDelta, schema_name));
}

std::shared_ptr<AvroTuple> AvroTuple::make(std::vector<avro::GenericRecord> v, std::string table_name, bool is_delta) {
    return std::make_shared<AvroTuple>(AvroTuple(v, table_name, is_delta));
}

