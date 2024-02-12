//
// Created by matt on 25/3/20.
//

#ifndef FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_LOCALPOPDIRECTORYENTRY_H
#define FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_LOCALPOPDIRECTORYENTRY_H

#include <fpdb/executor/physical/POpRelationshipType.h>
#include <caf/all.hpp>
#include <string>

namespace fpdb::executor::physical {

/**
 * An entry in the local physical operator directory
 */
class LocalPOpDirectoryEntry {

public:
  LocalPOpDirectoryEntry(std::string name,
                         ::caf::actor actor,
                         POpRelationshipType relationshipType,
                         int nodeId,
                         bool complete);
  LocalPOpDirectoryEntry() = default;
  LocalPOpDirectoryEntry(const LocalPOpDirectoryEntry&) = default;
  LocalPOpDirectoryEntry& operator=(const LocalPOpDirectoryEntry&) = default;

  bool complete() const;
  void complete(bool complete);

  const std::string &name() const;
  void name(const std::string &name);
  const ::caf::actor &getActor() const;
  int getNodeId() const;
  void destroyActor();
  POpRelationshipType relationshipType() const;
  void relationshipType(POpRelationshipType relationshipType);

private:
  std::string name_;
  ::caf::actor actor_;
  POpRelationshipType relationshipType_;
  int nodeId_;
  bool complete_;

// caf inspect
public:
  template <class Inspector>
  friend bool inspect(Inspector& f, LocalPOpDirectoryEntry& entry) {
    return f.object(entry).fields(f.field("name", entry.name_),
                                  f.field("actor", entry.actor_),
                                  f.field("relationshipType", entry.relationshipType_),
                                  f.field("nodeId", entry.nodeId_),
                                  f.field("complete", entry.complete_));
  }
};

}

#endif //FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_LOCALPOPDIRECTORYENTRY_H
