//
// Created by matt on 12/8/20.
//

#ifndef NORMAL_NORMAL_TUPLE_INCLUDE_NORMAL_TUPLE_FILEREADERBUILDER_H
#define NORMAL_NORMAL_TUPLE_INCLUDE_NORMAL_TUPLE_FILEREADERBUILDER_H

#include <string>

#include "FileReader.h"
#include "FileType.h"
#include "CSVReader.h"
#include "ParquetReader.h"

namespace normal::tuple {

class FileReaderBuilder {

public:
  static std::shared_ptr<FileReader> make(const std::string &path, FileType fileType);

};

}

#endif //NORMAL_NORMAL_TUPLE_INCLUDE_NORMAL_TUPLE_FILEREADERBUILDER_H
