//
// Created by Yifei Yang on 2/26/22.
//

#ifndef FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_FILE_REMOTEFILESCANPOP_H
#define FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_FILE_REMOTEFILESCANPOP_H

#include <fpdb/executor/physical/file/FileScanAbstractPOp.h>

namespace fpdb::executor::physical::file {

/**
 * Scan operator for remote files, i.e. pull-up (get) behavior
 */
class RemoteFileScanPOp: public FileScanAbstractPOp {

public:
  RemoteFileScanPOp(const std::string &name,
                    const std::vector<std::string> &columnNames,
                    int nodeId,
                    const std::string &bucket,
                    const std::string &object,
                    const std::shared_ptr<FileFormat> &format,
                    const std::shared_ptr<::arrow::Schema> &schema,
                    int64_t fileSize,
                    const std::string &host,
                    int port,
                    const std::optional<std::pair<int64_t, int64_t>> &byteRange = std::nullopt,
                    bool scanOnStart = true,
                    bool toCache = false);
  RemoteFileScanPOp() = default;
  RemoteFileScanPOp(const RemoteFileScanPOp&) = default;
  RemoteFileScanPOp& operator=(const RemoteFileScanPOp&) = default;
  ~RemoteFileScanPOp() = default;

  std::string getTypeString() const override;

  void setGetAdaptPushdownMetrics(bool getAdaptPushdownMetrics);

// caf inspect
public:
  template <class Inspector>
  friend bool inspect(Inspector& f, RemoteFileScanPOp& op) {
    return inspect_base_file_scan(f, op,
                                  f.field("getAdaptPushdownMetrics", op.getAdaptPushdownMetrics_));
  }

};

}


#endif //FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_FILE_REMOTEFILESCANPOP_H
