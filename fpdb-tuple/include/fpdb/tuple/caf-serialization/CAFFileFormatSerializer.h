//
// Created by Yifei Yang on 1/16/22.
//

#ifndef FPDB_FPDB_TUPLE_INCLUDE_FPDB_TUPLE_CAFSERIALIZATION_CAFFILEFORMATSERIALIZER_H
#define FPDB_FPDB_TUPLE_INCLUDE_FPDB_TUPLE_CAFSERIALIZATION_CAFFILEFORMATSERIALIZER_H

#include <fpdb/tuple/csv/CSVFormat.h>
#include <fpdb/tuple/parquet/ParquetFormat.h>
#include <fpdb/caf/CAFUtil.h>

using FileFormatPtr = std::shared_ptr<fpdb::tuple::FileFormat>;

CAF_BEGIN_TYPE_ID_BLOCK(FileFormat, fpdb::caf::CAFUtil::FileFormat_first_custom_type_id)
CAF_ADD_TYPE_ID(FileFormat, (FileFormatPtr))
CAF_ADD_TYPE_ID(FileFormat, (fpdb::tuple::csv::CSVFormat))
CAF_ADD_TYPE_ID(FileFormat, (fpdb::tuple::parquet::ParquetFormat))
CAF_END_TYPE_ID_BLOCK(FileFormat)

namespace caf {

template<>
struct variant_inspector_traits<FileFormatPtr> {
  using value_type = FileFormatPtr;

  // Lists all allowed types and gives them a 0-based index.
  static constexpr type_id_t allowed_types[] = {
          type_id_v<none_t>,
          type_id_v<fpdb::tuple::csv::CSVFormat>,
          type_id_v<fpdb::tuple::parquet::ParquetFormat>
  };

  // Returns which type in allowed_types corresponds to x.
  static auto type_index(const value_type &x) {
    if (!x)
      return 0;
    else if (x->getType() == fpdb::tuple::FileFormatType::CSV)
      return 1;
    else if (x->getType() == fpdb::tuple::FileFormatType::PARQUET)
      return 2;
    else
      return -1;
  }

  // Applies f to the value of x.
  template<class F>
  static auto visit(F &&f, const value_type &x) {
    switch (type_index(x)) {
      case 1:
        return f(static_cast<fpdb::tuple::csv::CSVFormat &>(*x));
      case 2:
        return f(static_cast<fpdb::tuple::parquet::ParquetFormat &>(*x));
      default: {
        none_t dummy;
        return f(dummy);
      }
    }
  }

  // Assigns a value to x.
  template<class U>
  static void assign(value_type &x, U value) {
    if constexpr (std::is_same<U, none_t>::value)
      x.reset();
    else
      x = std::make_shared<U>(std::move(value));
  }

  // Create a default-constructed object for `type` and then call the
  // continuation with the temporary object to perform remaining load steps.
  template<class F>
  static bool load(type_id_t type, F continuation) {
    switch (type) {
      default:
        return false;
      case type_id_v<none_t>: {
        none_t dummy;
        continuation(dummy);
        return true;
      }
      case type_id_v<fpdb::tuple::csv::CSVFormat>: {
        auto tmp = fpdb::tuple::csv::CSVFormat{};
        continuation(tmp);
        return true;
      }
      case type_id_v<fpdb::tuple::parquet::ParquetFormat>: {
        auto tmp = fpdb::tuple::parquet::ParquetFormat{};
        continuation(tmp);
        return true;
      }
    }
  }
};

template <>
struct inspector_access<FileFormatPtr> : variant_inspector_access<FileFormatPtr> {
  // nop
};

} // namespace caf

#endif //FPDB_FPDB_TUPLE_INCLUDE_FPDB_TUPLE_CAFSERIALIZATION_CAFFILEFORMATSERIALIZER_H
