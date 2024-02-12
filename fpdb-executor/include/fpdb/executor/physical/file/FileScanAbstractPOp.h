//
// Created by matt on 12/12/19.
//

#ifndef FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_FILE_FILESCANABSTRACTPOP_H
#define FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_FILE_FILESCANABSTRACTPOP_H

#include <fpdb/executor/physical/file/FileScanKernel.h>
#include <fpdb/executor/physical/PhysicalOp.h>

using namespace fpdb::executor::message;
using namespace fpdb::tuple;

namespace fpdb::executor::physical::file {

class FileScanAbstractPOp : public PhysicalOp {

public:
  FileScanAbstractPOp(const std::string &name,
                      POpType type,
                      const std::vector<std::string> &columnNames,
                      int nodeId,
                      const std::shared_ptr<FileScanKernel> &kernel,
                      bool scanOnStart,
                      bool toCache);
  FileScanAbstractPOp() = default;
  FileScanAbstractPOp(const FileScanAbstractPOp&) = default;
  FileScanAbstractPOp& operator=(const FileScanAbstractPOp&) = default;
  virtual ~FileScanAbstractPOp() = default;

  const std::shared_ptr<FileScanKernel> &getKernel() const;

  void onReceive(const Envelope &message) override;
  void clear() override;

protected:
  void onStart();
  void onCacheLoadResponse(const ScanMessage &message);
  void readAndSendTuples(const std::vector<std::string> &columnNames);
  std::shared_ptr<TupleSet> readTuples(const std::vector<std::string> &columnNames);
  void requestStoreSegmentsInCache(const std::shared_ptr<TupleSet> &tupleSet);

  std::shared_ptr<FileScanKernel> kernel_;
  bool scanOnStart_;
  bool toCache_;

  // for metrics of adaptive pushdown
  bool getAdaptPushdownMetrics_ = false;
};

}

#endif //FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_FILE_FILESCANABSTRACTPOP_H
