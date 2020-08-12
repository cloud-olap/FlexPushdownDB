//
// Created by matt on 12/8/20.
//

#include "normal/tuple/FileReaderBuilder.h"

using namespace normal::tuple;

std::shared_ptr<FileReader> FileReaderBuilder::make(const std::string &path, FileType fileType) {
  switch (fileType) {
  case FileType::CSV: return CSVReader::make(path).value();
  case FileType::Parquet: return ParquetReader::make(path).value();
  }
}
