//
// Created by matt on 11/8/20.
//

#ifndef NORMAL_NORMAL_TUPLE_INCLUDE_NORMAL_TUPLE_CONVERTER_H
#define NORMAL_NORMAL_TUPLE_INCLUDE_NORMAL_TUPLE_CONVERTER_H

#include <tl/expected.hpp>
#include <string>

#include <arrow/api.h>

namespace normal::tuple {

class Converter {

public:
  static tl::expected<void, std::string> csvToParquet(const std::string &inFile,
													  const std::string &outFile,
													  const ::arrow::Schema &schema,
													  int rowGroupSize);
}

}

#endif //NORMAL_NORMAL_TUPLE_INCLUDE_NORMAL_TUPLE_CONVERTER_H
