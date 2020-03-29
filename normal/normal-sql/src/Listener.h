//
// Created by matt on 26/3/20.
//

#ifndef NORMAL_NORMAL_SQL_SRC_LISTENER_H
#define NORMAL_NORMAL_SQL_SRC_LISTENER_H

#include <normal/sql/NormalSQLBaseListener.h>
#include <normal/core/OperatorManager.h>
#include <connector/Catalogue.h>

class Listener : public normal::sql::NormalSQLBaseListener {

private:
  std::shared_ptr<std::unordered_map<std::string, std::shared_ptr<Catalogue>>> catalogues_;
  std::shared_ptr<normal::core::OperatorManager> operatorManager_;

protected:
  void enterSelect_core(normal::sql::NormalSQLParser::Select_coreContext *Context) override;

public:
  explicit Listener(
      std::shared_ptr<std::unordered_map<std::string, std::shared_ptr<Catalogue>>> catalogues,
      std::shared_ptr<normal::core::OperatorManager> OperatorManager);
  ~Listener() override = default;

};

#endif //NORMAL_NORMAL_SQL_SRC_LISTENER_H
