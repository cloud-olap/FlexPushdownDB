//
// Created by matt on 5/12/19.
//

#ifndef NORMAL_NORMAL_CORE_SRC_S3SELECTSCAN_H
#define NORMAL_NORMAL_CORE_SRC_S3SELECTSCAN_H

#include <memory>
#include <string>

#include "normal/core/Operator.h"
#include "normal/core/TupleSet.h"

namespace normal::pushdown {

class S3SelectScan : public normal::core::Operator {
private:
  std::string m_s3Bucket;
  std::string m_s3Object;
  std::string m_sql;
  void onStart();

public:
  S3SelectScan(std::string name, std::string s3Bucket, std::string s3Object, std::string sql);
  ~S3SelectScan() override = default;

};

}

#endif //NORMAL_NORMAL_CORE_SRC_S3SELECTSCAN_H
