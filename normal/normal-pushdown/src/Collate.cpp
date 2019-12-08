//
// Created by matt on 5/12/19.
//

#include "normal/pushdown/Collate.h"
#include <spdlog/spdlog.h>
#include <iostream>

void Collate::onStart() {
}

void Collate::onStop() {
}

Collate::Collate(std::string name) : Operator(std::move(name)) {}

void Collate::onReceive(std::string msg) {
  spdlog::info("{}  |  Received", this->name());
  m_data.append(msg);
}
void Collate::show() {
  spdlog::info("{}  |  Show:\n{}", this->name(), m_data);
}
