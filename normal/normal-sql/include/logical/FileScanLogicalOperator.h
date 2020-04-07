//
// Created by matt on 7/4/20.
//

#ifndef NORMAL_NORMAL_SQL_SRC_AST_FILESCANLOGICALOPERATOR_H
#define NORMAL_NORMAL_SQL_SRC_AST_FILESCANLOGICALOPERATOR_H

#include "logical/ScanLogicalOperator.h"

class FileScanLogicalOperator: public ScanLogicalOperator {

private:
  std::string path_;

public:
  explicit FileScanLogicalOperator(std::string Path);

  [[nodiscard]] const std::string &path() const;

  std::shared_ptr<normal::core::Operator> toOperator() override;

};

#endif //NORMAL_NORMAL_SQL_SRC_AST_FILESCANLOGICALOPERATOR_H
