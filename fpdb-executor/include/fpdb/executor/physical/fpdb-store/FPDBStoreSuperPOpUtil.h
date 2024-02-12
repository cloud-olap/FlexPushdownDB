//
// Created by Yifei Yang on 3/31/22.
//

#ifndef FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_FPDB_STORE_FPDBSTORESUPERPOPUTIL_H
#define FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_FPDB_STORE_FPDBSTORESUPERPOPUTIL_H

#include <fpdb/executor/physical/fpdb-store/FPDBStoreSuperPOp.h>
#include <fpdb/executor/physical/filter/FilterPOp.h>
#include <fpdb/executor/physical/project/ProjectPOp.h>
#include <fpdb/executor/physical/aggregate/AggregatePOp.h>

namespace fpdb::executor::physical::fpdb_store {

class FPDBStoreSuperPOpUtil {

public:
  static std::vector<std::string>
  getPredicateColumnNames(const std::shared_ptr<FPDBStoreSuperPOp> &fpdbStoreSuperPOp);

  static std::vector<std::set<std::string>>
  getProjectColumnGroups(const std::shared_ptr<FPDBStoreSuperPOp> &fpdbStoreSuperPOp);

private:
  static void transformProjectColumnGroupsProject(const std::shared_ptr<project::ProjectPOp> &projectPOp,
                                                  std::vector<std::set<std::string>> &projectColumnGroups);

  static void transformProjectColumnGroupsAggregate(const std::shared_ptr<aggregate::AggregatePOp> &aggregatePOp,
                                                    std::vector<std::set<std::string>> &projectColumnGroups);


  /**
   * Replace elements in projectColumnGroups based on input and column names
   * @param inputColumnNameSets
   * @param outputColumnNames
   * @param projectColumnGroups
   * @return
   */
  static void transformProjectColumnGroups(const std::vector<std::set<std::string>> &inputColumnNameSets,
                                           const std::vector<std::string> &outputColumnNames,
                                           std::vector<std::set<std::string>> &projectColumnGroups);

};

}


#endif //FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_FPDB_STORE_FPDBSTORESUPERPOPUTIL_H
