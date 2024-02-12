//
// Created by Yifei Yang on 2/27/22.
//

#ifndef FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_FILE_REMOTEFILESCANKERNEL_H
#define FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_FILE_REMOTEFILESCANKERNEL_H

#include <fpdb/executor/physical/file/FileScanKernel.h>

namespace fpdb::executor::physical::file {

class RemoteFileScanKernel: public FileScanKernel {

public:
  RemoteFileScanKernel(const std::shared_ptr<FileFormat> &format,
                       const std::shared_ptr<::arrow::Schema> &schema,
                       int64_t fileSize,
                       const std::optional<std::pair<int64_t, int64_t>> &byteRange,
                       const std::string &bucket,
                       const std::string &object,
                       const std::string &host,
                       int port);
  RemoteFileScanKernel() = default;
  RemoteFileScanKernel(const RemoteFileScanKernel&) = default;
  RemoteFileScanKernel& operator=(const RemoteFileScanKernel&) = default;
  ~RemoteFileScanKernel() = default;

  static std::shared_ptr<RemoteFileScanKernel> make(const std::shared_ptr<FileFormat> &format,
                                                    const std::shared_ptr<::arrow::Schema> &schema,
                                                    int64_t fileSize,
                                                    const std::optional<std::pair<int64_t, int64_t>> &byteRange,
                                                    const std::string &bucket,
                                                    const std::string &object,
                                                    const std::string &host,
                                                    int port);

  const std::string &getBucket() const;
  const std::string &getObject() const;

  tl::expected<std::shared_ptr<TupleSet>, std::string> scan() override;
  tl::expected<std::shared_ptr<TupleSet>, std::string> scan(const std::vector<std::string> &columnNames) override;

private:
  std::string bucket_;
  std::string object_;
  std::string host_;
  int port_;

// caf inspect
public:
  template <class Inspector>
  friend bool inspect(Inspector& f, RemoteFileScanKernel& kernel) {
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
                                   f.field("bucket", kernel.bucket_),
                                   f.field("object", kernel.object_),
                                   f.field("host", kernel.host_),
                                   f.field("port", kernel.port_));
  }

};

}


#endif //FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_FILE_REMOTEFILESCANKERNEL_H
