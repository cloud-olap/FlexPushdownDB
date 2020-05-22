//
// Created by matt on 30/4/20.
//

#ifndef NORMAL_NORMAL_CORE_INCLUDE_NORMAL_CORE_ARROW_SCHEMAHELPER_H
#define NORMAL_NORMAL_CORE_INCLUDE_NORMAL_CORE_ARROW_SCHEMAHELPER_H

#include <memory>

#include <arrow/api.h>

namespace normal::tuple::arrow {

class SchemaHelper {

public:
  static std::shared_ptr<::arrow::Schema> concatenate(const std::vector<std::shared_ptr<::arrow::Schema>>& schemas);

};

}

#endif //NORMAL_NORMAL_CORE_INCLUDE_NORMAL_CORE_ARROW_SCHEMAHELPER_H
