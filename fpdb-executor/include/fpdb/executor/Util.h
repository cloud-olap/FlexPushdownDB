//
// Created by Yifei Yang on 10/28/23.
//

#ifndef FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_UTIL_H
#define FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_UTIL_H

#include <fpdb/caf/CAFUtil.h>
#include <string>

namespace fpdb::executor {

struct RemoteInfo {
  std::string host_;
  int port_;

// caf inspect
public:
  template <class Inspector>
  friend bool inspect(Inspector& f, RemoteInfo& info) {
    return f.object(info).fields(f.field("info", info.host_),
                                 f.field("info", info.port_));
  }
};

}

#endif //FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_UTIL_H
