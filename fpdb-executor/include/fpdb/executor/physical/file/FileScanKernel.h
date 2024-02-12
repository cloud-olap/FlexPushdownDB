//
// Created by matt on 12/8/20.
//

#ifndef FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_FILE_FILESCANKERNEL_H
#define FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_FILE_FILESCANKERNEL_H

#include <fpdb/executor/metrics/Globals.h>
#include <fpdb/catalogue/CatalogueEntryType.h>
#include <fpdb/tuple/FileFormat.h>
#include <fpdb/tuple/TupleSet.h>
#include <tl/expected.hpp>
#include <string>
#include <vector>
#include <memory>
#include <optional>

using namespace fpdb::catalogue;
using namespace fpdb::tuple;

namespace fpdb::executor::physical::file {

class FileScanKernel {

public:
  FileScanKernel(CatalogueEntryType type,
                 const std::shared_ptr<FileFormat> &format,
                 const std::shared_ptr<::arrow::Schema> &schema,
                 int64_t fileSize,
                 const std::optional<std::pair<int64_t, int64_t>> &byteRange);
  FileScanKernel() = default;
  FileScanKernel(const FileScanKernel&) = default;
  FileScanKernel& operator=(const FileScanKernel&) = default;
  virtual ~FileScanKernel() = default;

  CatalogueEntryType getType() const;
  const std::shared_ptr<FileFormat> &getFormat() const;
  const std::shared_ptr<::arrow::Schema> &getSchema() const;
  int64_t getFileSize() const;
  const std::optional<std::pair<int64_t, int64_t>> &getByteRange() const;

#if SHOW_DEBUG_METRICS == true
  int64_t getBytesReadLocal() const;
  int64_t getBytesReadRemote() const;
  void clearBytesRead();
#endif

  virtual tl::expected<std::shared_ptr<TupleSet>, std::string> scan() = 0;
  virtual tl::expected<std::shared_ptr<TupleSet>, std::string> scan(const std::vector<std::string> &columnNames) = 0;

protected:
  CatalogueEntryType type_;
  std::shared_ptr<FileFormat> format_;
  std::shared_ptr<::arrow::Schema> schema_;
  int64_t fileSize_;
  std::optional<std::pair<int64_t, int64_t>> byteRange_;

#if SHOW_DEBUG_METRICS == true
  int64_t bytesReadLocal_ = 0;
  int64_t bytesReadRemote_ = 0;
#endif

};

}

#endif //FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_FILE_FILESCANKERNEL_H
