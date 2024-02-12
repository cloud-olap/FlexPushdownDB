//
// Created by matt on 12/8/20.
//

#include <fpdb/tuple/LocalFileReaderBuilder.h>
#include <fpdb/tuple/csv/LocalCSVReader.h>
#include <fpdb/tuple/parquet/LocalParquetReader.h>

using namespace fpdb::tuple;

std::shared_ptr<LocalFileReader> LocalFileReaderBuilder::make(const std::shared_ptr<FileFormat> &format,
                                                              const std::shared_ptr<::arrow::Schema> &schema,
                                                              const std::string &path) {
  switch (format->getType()) {
    case FileFormatType::CSV:
      return csv::LocalCSVReader::make(format, schema, path);
    case FileFormatType::PARQUET:
      return parquet::LocalParquetReader::make(format, schema, path);
  }
}
