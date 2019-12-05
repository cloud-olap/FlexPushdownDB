//
// Created by matt on 5/12/19.
//

#ifndef NORMAL_NORMAL_CORE_SRC_S3SELECTSCAN_H
#define NORMAL_NORMAL_CORE_SRC_S3SELECTSCAN_H

#include <string>
#include "normal/core/Operator.h"
#include <normal/core/OperatorContext.h>

class S3SelectScan : public Operator {
private:
  std::string m_sql;
public:
  explicit S3SelectScan(std::string name);
  void onStart() override;
  void onStop() override;
  void onReceive(std::string msg) override;
};

#endif //NORMAL_NORMAL_CORE_SRC_S3SELECTSCAN_H
