//
// Created by Han Cao on 8/15/21.
//

#ifndef NORMAL_AVROTUPLE_H
#define NORMAL_AVROTUPLE_H


#include "avro/Generic.hh"

#include <string>
#include <vector>

namespace normal::avro_tuple {

class AvroTuple {
public:
    AvroTuple(std::vector<avro::GenericRecord> v, std::string table_name, bool is_delta);
    AvroTuple(bool isDelta, std::string table_name);

    static std::shared_ptr<AvroTuple> make(bool isDelta = false, std::string table_name = "table_name");
    static std::shared_ptr<AvroTuple> make(std::vector<avro::GenericRecord> v, std::string table_name, bool is_delta);

private:
    std::string schemaName_;
    std::vector<avro::GenericRecord> avroVector_;
    std::string tableName_;
    bool isDelta_;
};

}

#endif //NORMAL_AVROTUPLE_H
