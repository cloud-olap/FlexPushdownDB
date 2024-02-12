//
// Created by Yifei Yang on 2/26/22.
//

#include <fpdb/executor/physical/file/LocalFileScanPOp.h>
#include <fpdb/executor/physical/file/LocalFileScanKernel.h>

namespace fpdb::executor::physical::file {

LocalFileScanPOp::LocalFileScanPOp(const std::string &name,
                                   const std::vector<std::string> &columnNames,
                                   int nodeId,
                                   const std::string &path,
                                   const std::shared_ptr<FileFormat> &format,
                                   const std::shared_ptr<::arrow::Schema> &schema,
                                   int64_t fileSize,
                                   const std::optional<std::pair<int64_t, int64_t>> &byteRange,
                                   bool scanOnStart,
                                   bool toCache):
  FileScanAbstractPOp(name,
                      LOCAL_FILE_SCAN,
                      columnNames,
                      nodeId,
                      LocalFileScanKernel::make(format, schema, fileSize, byteRange, path),
                      scanOnStart,
                      toCache) {}

std::string LocalFileScanPOp::getTypeString() const {
  return "LocalFileScanPOp";
}

}
