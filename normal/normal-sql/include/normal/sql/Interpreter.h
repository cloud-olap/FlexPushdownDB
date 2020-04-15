//
// Created by matt on 26/3/20.
//

#ifndef NORMAL_NORMAL_SQL_INCLUDE_NORMAL_SQL_INTERPRETER_H
#define NORMAL_NORMAL_SQL_INCLUDE_NORMAL_SQL_INTERPRETER_H

#include <string>
#include <unordered_map>
#include <memory>

#include <normal/connector/Catalogue.h>
#include <normal/core/OperatorManager.h>

namespace normal::sql {

class Interpreter{

private:
  std::shared_ptr<std::unordered_map<std::string, std::shared_ptr<connector::Catalogue>>> catalogues_;
  std::shared_ptr<core::OperatorManager> operatorManager_;

public:
  Interpreter();
  [[nodiscard]] const std::shared_ptr<core::OperatorManager> &getOperatorManager() const;
  void parse(const std::string& sql);
  void put(const std::shared_ptr<connector::Catalogue> &catalogue);

};

}

#endif //NORMAL_NORMAL_SQL_INCLUDE_NORMAL_SQL_INTERPRETER_H
