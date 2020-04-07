//
// Created by matt on 27/3/20.
//

#ifndef NORMAL_NORMAL_SQL_SRC_CONNECTOR_LOCAL_FS_LOCALFILESYSTEMCONNECTOR_H
#define NORMAL_NORMAL_SQL_SRC_CONNECTOR_LOCAL_FS_LOCALFILESYSTEMCONNECTOR_H

#include <connector/Connector.h>

class LocalFileSystemConnector : public Connector {

private:

public:
  explicit LocalFileSystemConnector(const std::string &Name);
  ~LocalFileSystemConnector() override = default;
};

#endif //NORMAL_NORMAL_SQL_SRC_CONNECTOR_LOCAL_FS_LOCALFILESYSTEMCONNECTOR_H
