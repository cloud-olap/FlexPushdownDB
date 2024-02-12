//
// Created by Yifei Yang on 2/13/22.
//

#ifndef FPDB_FPDB_TUPLE_INCLUDE_FPDB_TUPLE_FILEFORMAT_H
#define FPDB_FPDB_TUPLE_INCLUDE_FPDB_TUPLE_FILEFORMAT_H

#include <fpdb/tuple/FileFormatType.h>
#include <nlohmann/json.hpp>
#include <tl/expected.hpp>

namespace fpdb::tuple {

class FileFormat {

public:
  FileFormat(FileFormatType type);
  FileFormat() = default;
  FileFormat(const FileFormat&) = default;
  FileFormat& operator=(const FileFormat&) = default;
  virtual ~FileFormat() = default;

  FileFormatType getType() const;

  virtual bool isColumnar() const = 0;

  virtual ::nlohmann::json toJson() const = 0;
  static tl::expected<std::shared_ptr<FileFormat>, std::string> fromJson(const nlohmann::json &jObj);

protected:
  FileFormatType type_;

};

}

#endif // FPDB_FPDB_TUPLE_INCLUDE_FPDB_TUPLE_FILEFORMAT_H
