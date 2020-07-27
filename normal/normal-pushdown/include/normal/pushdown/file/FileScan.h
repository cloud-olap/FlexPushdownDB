//
// Created by matt on 12/12/19.
//

#ifndef NORMAL_NORMAL_NORMAL_PUSHDOWN_FILESCAN_H
#define NORMAL_NORMAL_NORMAL_PUSHDOWN_FILESCAN_H

#include <string>

#include <normal/core/Operator.h>
#include <normal/core/cache/LoadResponseMessage.h>
#include <normal/pushdown/scan/ScanMessage.h>

using namespace normal::tuple;
using namespace normal::pushdown::scan;

namespace normal::pushdown {

class FileScan : public normal::core::Operator {

public:
  [[deprecated ("Use constructor accepting a byte range")]] FileScan(std::string name, std::string filePath, long queryId);

  FileScan(std::string name,
		   std::string filePath,
		   std::vector<std::string> columnNames,
		   unsigned long startOffset,
		   unsigned long finishOffset,
		   long queryId,
		   bool scanOnStart = false);

  static std::shared_ptr<FileScan> make(std::string name,
										std::string filePath,
										std::vector<std::string> columnNames,
										unsigned long startOffset,
										unsigned long finishOffset,
										long queryId,
										bool scanOnStart = false);

private:
  std::string filePath_;
  std::vector<std::string> columnNames_;
  unsigned long startOffset_;
  unsigned long finishOffset_;
  long queryId_;
  bool scanOnStart_;

  void onStart();
  void onReceive(const normal::core::message::Envelope &message) override;
  void onCacheLoadResponse(const ScanMessage &Message);

  tl::expected<std::shared_ptr<TupleSet2>, std::string> readCSVFile(const std::vector<std::string> &columnNames);

  void requestStoreSegmentsInCache(const std::shared_ptr<TupleSet2> &tupleSet);
  void readAndSendTuples(const std::vector<std::string> &columnNames);
};

}

#endif //NORMAL_NORMAL_NORMAL_PUSHDOWN_FILESCAN_H
