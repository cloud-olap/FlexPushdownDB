//
// Created by matt on 27/3/20.
//

#include <normal/sql/connector/Connector.h>

#include <utility>

normal::sql::connector::Connector::Connector(std::string name) :
    name_(std::move(name)) {
}
