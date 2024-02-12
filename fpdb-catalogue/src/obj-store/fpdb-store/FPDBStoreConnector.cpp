//
// Created by Yifei Yang on 3/1/22.
//

#include <fpdb/catalogue/obj-store/fpdb-store/FPDBStoreConnector.h>
#include <fmt/format.h>

namespace fpdb::catalogue::obj_store {

FPDBStoreConnector::FPDBStoreConnector(const std::vector<std::string> &hosts,
                                       int fileServicePort,
                                       int flightPort):
  ObjStoreConnector(ObjStoreType::FPDB_STORE),
  hosts_(hosts),
  fileServicePort_(fileServicePort),
  flightPort_(flightPort) {}

const std::vector<std::string> &FPDBStoreConnector::getHosts() const {
  return hosts_;
}

const std::string &FPDBStoreConnector::getHost(int id) const {
  if (id >= (int) hosts_.size()) {
    throw std::runtime_error(fmt::format("Invalid host id '{}', num hosts: '{}'", id, hosts_.size()));
  }
  return hosts_[id];
}

size_t FPDBStoreConnector::getNumHosts() const {
  return hosts_.size();
}

int FPDBStoreConnector::getFileServicePort() const {
  return fileServicePort_;
}

int FPDBStoreConnector::getFlightPort() const {
  return flightPort_;
}

}
