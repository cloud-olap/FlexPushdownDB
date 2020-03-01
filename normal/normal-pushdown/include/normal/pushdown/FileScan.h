//
// Created by matt on 12/12/19.
//

#ifndef NORMAL_NORMAL_NORMAL_PUSHDOWN_FILESCAN_H
#define NORMAL_NORMAL_NORMAL_PUSHDOWN_FILESCAN_H

#include <string>

#include <normal/core/Operator.h>

class FileScan : public normal::core::Operator {
private:
  std::string m_filePath;
protected:
  void onStart() override;
  void onStop() override;
public:
  FileScan(std::string name, std::string filePath);
  ~FileScan() override;
};

#endif //NORMAL_NORMAL_NORMAL_PUSHDOWN_FILESCAN_H
