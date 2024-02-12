//
// Created by matt on 12/8/20.
//

#ifndef FPDB_FPDB_TUPLE_INCLUDE_FPDB_TUPLE_LOCALFILEREADERBUILDER_H
#define FPDB_FPDB_TUPLE_INCLUDE_FPDB_TUPLE_LOCALFILEREADERBUILDER_H

#include <fpdb/tuple/LocalFileReader.h>
#include <fpdb/tuple/FileFormat.h>

namespace fpdb::tuple {

class LocalFileReaderBuilder {

public:
  static std::shared_ptr<LocalFileReader> make(const std::shared_ptr<FileFormat> &format,
                                               const std::shared_ptr<::arrow::Schema> &schema,
                                               const std::string &path);

};

}

#endif //FPDB_FPDB_TUPLE_INCLUDE_FPDB_TUPLE_LOCALFILEREADERBUILDER_H
