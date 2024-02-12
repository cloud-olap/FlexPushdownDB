//
// Created by Yifei Yang on 12/17/21.
//

#include <fpdb/plan/Util.h>

namespace fpdb::plan {

void Util::renameColumns(set<string> &columnNames, const unordered_map<string, string> &renames) {
  for (const auto &rename: renames) {
    if (columnNames.find(rename.first) != columnNames.end()) {
      columnNames.erase(rename.first);
      columnNames.emplace(rename.second);
    }
  }
}

void Util::deRenameColumns(set<string> &renamedColumnNames, const unordered_map<string, string> &renames) {
  set<string> deRenamedColumnNames;
  for (const auto &rename: renames) {
    if (renamedColumnNames.find(rename.second) != renamedColumnNames.end()) {
      renamedColumnNames.erase(rename.second);
      deRenamedColumnNames.emplace(rename.first);
    }
  }
  renamedColumnNames.insert(deRenamedColumnNames.begin(), deRenamedColumnNames.end());
}

}
