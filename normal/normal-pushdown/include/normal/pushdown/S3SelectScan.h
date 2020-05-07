//
// Created by matt on 5/12/19.
//

#ifndef NORMAL_NORMAL_CORE_SRC_S3SELECTSCAN_H
#define NORMAL_NORMAL_CORE_SRC_S3SELECTSCAN_H

#include <memory>
#include <string>

#include <aws/s3/S3Client.h>
#include <normal/core/Cache.h>
#include <normal/core/message/TupleMessage.h>

#include "normal/core/Operator.h"
#include "normal/core/TupleSet.h"

namespace normal::pushdown {

class S3SelectScan : public normal::core::Operator {
private:
  std::string s3Bucket_;
  std::string s3Object_;
  std::string sql_;
  std::shared_ptr<Cache> m_cache;
  std::string m_col;
  std::string m_tbl;
  std::shared_ptr<Aws::S3::S3Client> s3Client_;
  bool cacheSuccess_ = false;

  void onStart();

public:
  S3SelectScan(std::string name, std::string s3Bucket, std::string s3Object, std::string sql, std::string m_tbl, std::string m_col, std::shared_ptr<Aws::S3::S3Client> s3Client);
  ~S3SelectScan() override = default;
  std::shared_ptr<Cache> getCache(){
      return m_cache;
  }
  void onReceive(const normal::core::message::Envelope &message) override;
  void onTuple(const core::message::TupleMessage &message);
};

}

#endif //NORMAL_NORMAL_CORE_SRC_S3SELECTSCAN_H
