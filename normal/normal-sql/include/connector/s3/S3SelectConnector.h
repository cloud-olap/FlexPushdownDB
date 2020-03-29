//
// Created by matt on 27/3/20.
//

#ifndef NORMAL_NORMAL_SQL_SRC_S3SELECTCONNECTOR_H
#define NORMAL_NORMAL_SQL_SRC_S3SELECTCONNECTOR_H

#include "connector/Connector.h"

class S3SelectConnector : public Connector {

private:

public:
  explicit S3SelectConnector(const std::string &name);
  ~S3SelectConnector() override = default;

};

#endif //NORMAL_NORMAL_SQL_SRC_S3SELECTCONNECTOR_H
