//
// Created by matt on 12/8/20.
//

#ifndef NORMAL_NORMAL_TUPLE_INCLUDE_NORMAL_TUPLE_FILEREADER_H
#define NORMAL_NORMAL_TUPLE_INCLUDE_NORMAL_TUPLE_FILEREADER_H

#include <memory>

#include <tl/expected.hpp>

#include "TupleSet2.h"

namespace normal::tuple {

class FileReader {
public:

  virtual ~FileReader() = default;

  virtual tl::expected<std::shared_ptr<TupleSet2>, std::string>
  read(const std::vector<std::string> &columnNames, unsigned long startPos, unsigned long finishPos) = 0;

};

}

#endif //NORMAL_NORMAL_TUPLE_INCLUDE_NORMAL_TUPLE_FILEREADER_H
