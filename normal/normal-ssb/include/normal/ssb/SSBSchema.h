//
// Created by matt on 10/8/20.
//

#ifndef NORMAL_NORMAL_SSB_INCLUDE_NORMAL_SSB_SSBSCHEMA_H
#define NORMAL_NORMAL_SSB_INCLUDE_NORMAL_SSB_SSBSCHEMA_H

#include <vector>
#include <string>
#include <normal/tuple/Schema.h>

class SSBSchema {
public:
  static std::shared_ptr<arrow::Schema> customer();
  static std::shared_ptr<arrow::Schema> date();
  static std::shared_ptr<arrow::Schema> lineOrder();
  static std::shared_ptr<arrow::Schema> part();
  static std::shared_ptr<arrow::Schema> supplier();
};

#endif //NORMAL_NORMAL_SSB_INCLUDE_NORMAL_SSB_SSBSCHEMA_H
