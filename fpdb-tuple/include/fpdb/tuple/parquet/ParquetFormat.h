//
// Created by Yifei Yang on 2/13/22.
//

#ifndef FPDB_FPDB_TUPLE_INCLUDE_FPDB_TUPLE_PARQUET_PARQUETFORMAT_H
#define FPDB_FPDB_TUPLE_INCLUDE_FPDB_TUPLE_PARQUET_PARQUETFORMAT_H

#include <fpdb/tuple/FileFormat.h>

namespace fpdb::tuple::parquet {

class ParquetFormat : public FileFormat {

public:
  ParquetFormat();
  ParquetFormat(const ParquetFormat&) = default;
  ParquetFormat& operator=(const ParquetFormat&) = default;
  ~ParquetFormat() = default;

  bool isColumnar() const override;

  ::nlohmann::json toJson() const override;
  static tl::expected<std::shared_ptr<ParquetFormat>, std::string> fromJson(const nlohmann::json &jObj);

// caf inspect
public:
  template <class Inspector>
  friend bool inspect(Inspector& f, ParquetFormat& format) {
    return f.apply(format.type_);
  }
};

}

#endif // FPDB_FPDB_TUPLE_INCLUDE_FPDB_TUPLE_PARQUET_PARQUETFORMAT_H
