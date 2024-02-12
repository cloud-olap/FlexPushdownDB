//
// Created by matt on 12/8/20.
//

#ifndef FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_FILE_LOCALFILESCANKERNEL_H
#define FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_FILE_LOCALFILESCANKERNEL_H

#include <fpdb/executor/physical/file/FileScanKernel.h>

namespace fpdb::executor::physical::file {

class LocalFileScanKernel: public FileScanKernel {

public:
  LocalFileScanKernel(const std::shared_ptr<FileFormat> &format,
                      const std::shared_ptr<::arrow::Schema> &schema,
                      int64_t fileSize,
                      const std::optional<std::pair<int64_t, int64_t>> &byteRange,
                      const std::string &path);
  LocalFileScanKernel() = default;
  LocalFileScanKernel(const LocalFileScanKernel&) = default;
  LocalFileScanKernel& operator=(const LocalFileScanKernel&) = default;
  ~LocalFileScanKernel() = default;

  static std::shared_ptr<LocalFileScanKernel> make(const std::shared_ptr<FileFormat> &format,
                                                   const std::shared_ptr<::arrow::Schema> &schema,
                                                   int64_t fileSize,
                                                   const std::optional<std::pair<int64_t, int64_t>> &byteRange,
                                                   const std::string &path);

  const std::string &getPath() const;
  void setPath(const std::string &path);

  tl::expected<std::shared_ptr<TupleSet>, std::string> scan() override;
  tl::expected<std::shared_ptr<TupleSet>, std::string> scan(const std::vector<std::string> &columnNames) override;

private:
  std::string path_;

// caf inspect
public:
  template <class Inspector>
  friend bool inspect(Inspector& f, LocalFileScanKernel& kernel) {
    auto schemaToBytes = [&kernel]() -> decltype(auto) {
      return fpdb::tuple::ArrowSerializer::schema_to_bytes(kernel.schema_);
    };
    auto schemaFromBytes = [&kernel](const std::vector<std::uint8_t> &bytes) {
      kernel.schema_ = ArrowSerializer::bytes_to_schema(bytes);
      return true;
    };
    return f.object(kernel).fields(f.field("type", kernel.type_),
                                   f.field("format", kernel.format_),
                                   f.field("schema", schemaToBytes, schemaFromBytes),
                                   f.field("fileSize", kernel.fileSize_),
                                   f.field("byteRange", kernel.byteRange_),
                                   f.field("path", kernel.path_));
  }
};

}

#endif //FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_FILE_LOCALFILESCANKERNEL_H
