//
// Created by matt on 12/8/20.
//

#ifndef NORMAL_NORMAL_TUPLE_INCLUDE_NORMAL_TUPLE_CSVREADER_H
#define NORMAL_NORMAL_TUPLE_INCLUDE_NORMAL_TUPLE_CSVREADER_H

#include <string>
#include <memory>

#include <tl/expected.hpp>
#include <arrow/io/api.h>
#include <parquet/arrow/reader.h>

#include "FileReader.h"
#include "TupleSet2.h"

namespace normal::tuple {

class CSVReader : public FileReader {

public:
  explicit CSVReader(std::string path);

  static tl::expected<std::shared_ptr<CSVReader>, std::string> make(const std::string &path);

  [[nodiscard]] tl::expected<std::shared_ptr<TupleSet2>, std::string>
  read(const std::vector<std::string> &columnNames, unsigned long startPos, unsigned long finishPos) override;

private:
  std::string path_;

};

}

#endif //NORMAL_NORMAL_TUPLE_INCLUDE_NORMAL_TUPLE_CSVREADER_H
