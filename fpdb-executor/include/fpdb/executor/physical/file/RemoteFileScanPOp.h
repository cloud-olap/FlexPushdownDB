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
    return f.object(op).fields(f.field("name", op.name_),
                               f.field("type", op.type_),
                               f.field("projectColumnNames", op.projectColumnNames_),
                               f.field("nodeId", op.nodeId_),
                               f.field("queryId", op.queryId_),
                               f.field("opContext", op.opContext_),
                               f.field("producers", op.producers_),
                               f.field("consumers", op.consumers_),
                               f.field("consumerToBloomFilterInfo", op.consumerToBloomFilterInfo_),
                               f.field("isSeparated", op.isSeparated_),
                               f.field("kernel", op.kernel_),
                               f.field("scanOnStart", op.scanOnStart_),
                               f.field("toCache", op.toCache_),
                               f.field("getAdaptPushdownMetrics", op.getAdaptPushdownMetrics_));
  }

};

}


#endif //FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_FILE_REMOTEFILESCANPOP_H
