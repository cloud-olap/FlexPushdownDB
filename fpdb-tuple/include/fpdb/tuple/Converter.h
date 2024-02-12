//
// Created by matt on 11/8/20.
//

#ifndef FPDB_FPDB_TUPLE_INCLUDE_FPDB_TUPLE_CONVERTER_H
#define FPDB_FPDB_TUPLE_INCLUDE_FPDB_TUPLE_CONVERTER_H

#include "fpdb/tuple/csv/CSVFormat.h"
#include <tl/expected.hpp>
#include <string>

#include <arrow/api.h>
#include <parquet/arrow/writer.h>

namespace fpdb::tuple {

class Converter {

public:
  static tl::expected<void, std::string> csvToParquet(
          const std::string &inFile,
          const std::string &outFile,
          const std::shared_ptr<csv::CSVFormat> &csvFormat,
          const std::shared_ptr<::arrow::Schema> &schema,
          int rowGroupSize,
          ::parquet::Compression::type compressionType = ::parquet::Compression::type::UNCOMPRESSED);
};

}

#endif //FPDB_FPDB_TUPLE_INCLUDE_FPDB_TUPLE_CONVERTER_H
