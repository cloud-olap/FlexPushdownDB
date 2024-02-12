//
// Created by Yifei Yang on 6/26/22.
//

#ifndef FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_FPDBSTOREEXECUTION_H
#define FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_FPDBSTOREEXECUTION_H

#include <fpdb/executor/Execution.h>

namespace fpdb::executor {

class FPDBStoreExecution: public Execution {

public:
  using TableCallBack = std::function<void(const std::string &consumer, const std::shared_ptr<arrow::Table> &table)>;
  using BitmapCallBack = std::function<void(const std::string &sender, const std::vector<int64_t> &bitmap)>;

  FPDBStoreExecution(long queryId,
                     const std::shared_ptr<::caf::actor_system> &actorSystem,
                     const std::shared_ptr<PhysicalPlan> &physicalPlan,
                     TableCallBack tableCallBack,
                     BitmapCallBack bitmapCallBack);
  ~FPDBStoreExecution() override = default;

private:
  void join() override;

  TableCallBack tableCallBack_;
  BitmapCallBack bitmapCallBack_;
};

}


#endif //FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_FPDBSTOREEXECUTION_H
