//
// Created by Yifei Yang on 12/17/21.
//

#ifndef FPDB_FPDB_PLAN_INCLUDE_FPDB_PLAN_UTIL_H
#define FPDB_FPDB_PLAN_INCLUDE_FPDB_PLAN_UTIL_H

#include <set>
#include <unordered_map>
#include <memory>
#include <string>

using namespace std;

namespace fpdb::plan {

class Util {

public:
  static void renameColumns(set<string> &columnNames, const unordered_map<string, string> &renames);
  static void deRenameColumns(set<string> &renamedColumnNames, const unordered_map<string, string> &renames);

};

}


#endif //FPDB_FPDB_PLAN_INCLUDE_FPDB_PLAN_UTIL_H
