//
// Created by matt on 7/4/20.
//

#ifndef NORMAL_NORMAL_SQL_INCLUDE_NORMAL_SQL_LOGICAL_FILESCANLOGICALOPERATOR_H
#define NORMAL_NORMAL_SQL_INCLUDE_NORMAL_SQL_LOGICAL_FILESCANLOGICALOPERATOR_H

#include <string>

#include <normal/core/Operator.h>

#include <normal/sql/logical/ScanLogicalOperator.h>

namespace normal::sql::logical {

class FileScanLogicalOperator: public ScanLogicalOperator {

private:
  std::string path_;

public:
  explicit FileScanLogicalOperator(std::string Path);

  [[nodiscard]] const std::string &path() const;

  std::shared_ptr<core::Operator> toOperator() override;

};

}

#endif //NORMAL_NORMAL_SQL_INCLUDE_NORMAL_SQL_LOGICAL_FILESCANLOGICALOPERATOR_H
