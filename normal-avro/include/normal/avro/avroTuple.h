//
// Created by Han Cao on 8/15/21.
//

#ifndef NORMAL_AVROTUPLE_H
#define NORMAL_AVROTUPLE_H

#include "avro/Generic.hh"

namespace normal::avro_tuple {

    class avroTuple {
    public:
        avroTuple(std::vector<avro::GenericRecord> v, std::string table_name, bool is_delta);
        avroTuple(bool isDelta, std::string table_name);

        static std::shared_ptr<avroTuple> make(bool isDelta = false, std::string table_name);
        static std::shared_ptr<avroTuple> make(std::vector<avro::GenericRecord> v, std::string table_name, bool is_delta);

    private:
        std::string schemaName_;
        std::vector<avro::GenericRecord> avroVector_;
        std::string tableName_;
        bool isDelta_;
    };

}

#endif //NORMAL_AVROTUPLE_H
