//
// Created by matt on 27/3/20.
//

#include "connector/Connector.h"

#include <utility>

Connector::Connector(std::string name) :
    name_(std::move(name)) {
}
