//
// Created by Yifei Yang on 10/30/21.
//

#ifndef FPDB_FPDB_CALCITE_CPP_INCLUDE_FPDB_CALCITE_CALCITECONFIG_H
#define FPDB_FPDB_CALCITE_CPP_INCLUDE_FPDB_CALCITE_CALCITECONFIG_H

#include <string>
#include <memory>

using namespace std;

namespace fpdb::calcite {

class CalciteConfig {
public:
  CalciteConfig(int port, string jarName);

  static std::shared_ptr<CalciteConfig> parseCalciteConfig();

  int getPort() const;
  const string &getJarName() const;

private:
  int port_;
  string jarName_;
};

}


#endif //FPDB_FPDB_CALCITE_CPP_INCLUDE_FPDB_CALCITE_CALCITECONFIG_H
