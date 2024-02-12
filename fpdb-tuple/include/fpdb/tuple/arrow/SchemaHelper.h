//
// Created by matt on 30/4/20.
//

#ifndef FPDB_FPDB_CORE_INCLUDE_FPDB_CORE_ARROW_SCHEMAHELPER_H
#define FPDB_FPDB_CORE_INCLUDE_FPDB_CORE_ARROW_SCHEMAHELPER_H

#include <memory>

#include <arrow/api.h>

namespace fpdb::tuple::arrow {

class SchemaHelper {

public:
  static std::shared_ptr<::arrow::Schema> concatenate(const std::vector<std::shared_ptr<::arrow::Schema>>& schemas);

};

}

#endif //FPDB_FPDB_CORE_INCLUDE_FPDB_CORE_ARROW_SCHEMAHELPER_H
