//
// Created by matt on 27/3/20.
//

#ifndef NORMAL_NORMAL_SQL_INCLUDE_NORMAL_SQL_CONNECTOR_CONNECTOR_H
#define NORMAL_NORMAL_SQL_INCLUDE_NORMAL_SQL_CONNECTOR_CONNECTOR_H

#include <string>

namespace normal::connector {

class Connector {

private:
  std::string name_;

public:
  explicit Connector(std::string name);
  virtual ~Connector() = default;

};

}

#endif //NORMAL_NORMAL_SQL_INCLUDE_NORMAL_SQL_CONNECTOR_CONNECTOR_H
