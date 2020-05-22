//
// Created by matt on 12/12/19.
//

#ifndef NORMAL_NORMAL_NORMAL_PUSHDOWN_FILESCAN_H
#define NORMAL_NORMAL_NORMAL_PUSHDOWN_FILESCAN_H

#include <string>

#include <normal/core/Operator.h>
#include <normal/core/cache/LoadResponseMessage.h>

using namespace normal::tuple;

namespace normal::pushdown {

class FileScan : public normal::core::Operator {

public:
  [[ deprecated ("Use constructor that accepting a byte range")]] FileScan(std::string name, std::string filePath);
  FileScan(std::string name, std::string filePath, unsigned long startOffset, unsigned long finishOffset);

  void requestCachedSegment();
  void onCacheLoadResponse(const normal::core::cache::LoadResponseMessage& Message);
  tl::expected<std::shared_ptr<TupleSet>, std::string> readCSVFile();

private:
  std::string filePath_;
  unsigned long startOffset_;
  unsigned long finishOffset_;
  void onStart();
  void onReceive(const normal::core::message::Envelope &message) override;

};

}

#endif //NORMAL_NORMAL_NORMAL_PUSHDOWN_FILESCAN_H
