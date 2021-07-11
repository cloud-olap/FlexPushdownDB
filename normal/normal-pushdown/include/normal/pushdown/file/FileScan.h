//
// Created by matt on 12/12/19.
//

#ifndef NORMAL_NORMAL_NORMAL_PUSHDOWN_FILESCAN_H
#define NORMAL_NORMAL_NORMAL_PUSHDOWN_FILESCAN_H

#include <string>

#include <normal/core/Operator.h>
#include <normal/core/cache/LoadResponseMessage.h>
#include <normal/pushdown/scan/ScanMessage.h>
#include <normal/core/message/CompleteMessage.h>
#include <normal/pushdown/Forward.h>
#include "FileScanKernel.h"

using namespace normal::tuple;
using namespace normal::pushdown::scan;

namespace normal::pushdown::file {

class FileScan : public normal::core::Operator {

public:
  [[deprecated ("Use constructor accepting a byte range")]]
  FileScan(std::string name, const std::string &filePath, long queryId);

  FileScan(std::string name,
		   const std::string &filePath,
		   FileType fileType,
		   std::vector<std::string> columnNames,
		   unsigned long startOffset,
		   unsigned long finishOffset,
		   long queryId,
		   bool scanOnStart = false);

  static std::shared_ptr<FileScan> make(const std::string &name,
										const std::string &filePath,
										const std::vector<std::string> &columnNames,
										unsigned long startOffset,
										unsigned long finishOffset,
										long queryId = 0,
										bool scanOnStart = false);

  static std::shared_ptr<FileScan> make(const std::string &name,
										const std::string &filePath,
										FileType fileType,
										const std::vector<std::string> &columnNames,
										unsigned long startOffset,
										unsigned long finishOffset,
										long queryId = 0,
										bool scanOnStart = false);

  void onReceive(const normal::core::message::Envelope &message) override;

private:
  bool scanOnStart_;
  std::vector<std::string> columnNames_;
  std::unique_ptr<FileScanKernel> kernel_;
public:
  [[nodiscard]] const std::unique_ptr<FileScanKernel> &getKernel() const;
  [[nodiscard]] bool isScanOnStart() const;
  [[nodiscard]] const std::vector<std::string> &getColumnNames() const;
private:
  void onStart();

  void onCacheLoadResponse(const ScanMessage &Message);
  void onComplete(const normal::core::message::CompleteMessage &message);

  void requestStoreSegmentsInCache(const std::shared_ptr<TupleSet2> &tupleSet);
  void readAndSendTuples(const std::vector<std::string> &columnNames);
};

}

#endif //NORMAL_NORMAL_NORMAL_PUSHDOWN_FILESCAN_H
