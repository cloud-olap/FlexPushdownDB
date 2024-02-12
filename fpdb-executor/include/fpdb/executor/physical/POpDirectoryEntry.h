//
// Created by matt on 24/3/20.
//

#ifndef FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_POPDIRECTORYENTRY_H
#define FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_POPDIRECTORYENTRY_H

#include <fpdb/executor/physical/POpContext.h>
#include <caf/all.hpp>
#include <string>

namespace fpdb::executor::physical {

/**
 * Entry in the physical operator directory
 */
class POpDirectoryEntry {

private:
  std::shared_ptr<PhysicalOp> def_;
  ::caf::actor actorHandle_;
  bool complete_;

public:
  POpDirectoryEntry(std::shared_ptr<PhysicalOp> def, ::caf::actor actorHandle, bool complete);
  [[nodiscard]] const std::shared_ptr<PhysicalOp> &getDef() const;
  [[nodiscard]] const ::caf::actor &getActorHandle() const;
  [[nodiscard]] bool isComplete() const;
  void setDef(const std::shared_ptr<PhysicalOp> &def);
  void setActorHandle(const ::caf::actor &actorHandle);
  void setComplete(bool complete);
};

}

#endif //FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_POPDIRECTORYENTRY_H
