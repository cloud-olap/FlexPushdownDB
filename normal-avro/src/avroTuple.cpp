//
// Created by Han Cao on 8/15/21.
//

#include "../include/normal/avro/avroTuple.h"

normal::avro::avroTuple::avroTuple(bool isDelta, std::string table_name) : tableName_(table_name) isDelta_(isDelta) {
}

std::shared_ptr<avroTuple> normal::avro::avroTuple::make(bool isDelta, std::string schema_name) {
    return std::make_shared<avroTuple>(avroTuple(isDelta, schema_name));
}

normal::avro::avroTuple::avroTuple(std::vector<avro::GenericRecord> v, std::string table_name, bool is_delta) {
    avroVector_ = v;
    tableName_ = table_name;
    isDelta = is_delta;
}

std::shared_ptr<avroTuple> make(std::vector<avro::GenericRecord> v, std::string table_name, bool is_delta) {
    return std::make_shared<avroTuple>(avroTuple(v, table_name, is_delta));
}
