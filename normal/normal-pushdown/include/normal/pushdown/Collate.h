//
// Created by matt on 5/12/19.
//

#ifndef NORMAL_NORMAL_S3_SRC_COLLATE_H
#define NORMAL_NORMAL_S3_SRC_COLLATE_H

#include <string>
#include "normal/core/Operator.h"
#include <normal/core/OperatorContext.h>

class Collate : public Operator {
private:
  std::string m_data;
public:
  explicit Collate(std::string name);
  void onStart() override;
  void onStop() override;
  void onReceive(std::string msg) override;
  void show();
};

#endif //NORMAL_NORMAL_S3_SRC_COLLATE_H
