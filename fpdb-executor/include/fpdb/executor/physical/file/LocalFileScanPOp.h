//
// Created by Yifei Yang on 2/26/22.
//

#ifndef FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_FILE_LOCALFILESCANPOP_H
#define FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_FILE_LOCALFILESCANPOP_H

#include <fpdb/executor/physical/file/FileScanAbstractPOp.h>

namespace fpdb::executor::physical::file {

/**
 * Scan operator for local files
 */
class LocalFileScanPOp: public FileScanAbstractPOp {

public:
  LocalFileScanPOp(const std::string &name,
                   const std::vector<std::string> &columnNames,
                   int nodeId,
                   const std::string &path,
                   const std::shared_ptr<FileFormat> &format,
                   const std::shared_ptr<::arrow::Schema> &schema,
                   int64_t fileSize,
                   const std::optional<std::pair<int64_t, int64_t>> &byteRange = std::nullopt,
                   bool scanOnStart = true,
                   bool toCache = false);
  LocalFileScanPOp() = default;
  LocalFileScanPOp(const LocalFileScanPOp&) = default;
  LocalFileScanPOp& operator=(const LocalFileScanPOp&) = default;
  ~LocalFileScanPOp() = default;

  std::string getTypeString() const override;

// caf inspect
public:
  template <class Inspector>
  friend bool inspect(Inspector& f, LocalFileScanPOp& op) {
    return inspect_base_file_scan(f, op);
  }
};

}


#endif //FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_FILE_LOCALFILESCANPOP_H
