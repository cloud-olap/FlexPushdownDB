//
// Created by matt on 15/4/20.
//

#ifndef NORMAL_NORMAL_SQL_INCLUDE_NORMAL_SQL_CONNECTOR_PARTITION_PARTITION_H
#define NORMAL_NORMAL_SQL_INCLUDE_NORMAL_SQL_CONNECTOR_PARTITION_PARTITION_H

#include <memory>
#include <string>
#include <optional>

/**
 * Base class for partition meta data
 *
 * TODO: Just a placeholder at the moment
 */
class Partition {
public:
  virtual ~Partition() = default;

  virtual std::string toString() = 0;

  virtual bool equalTo(std::shared_ptr<Partition> other) = 0;

  virtual size_t hash() = 0;

  [[nodiscard]] const long &getNumBytes() const;
  void setNumBytes(const long &NumBytes);

private:
  long numBytes = -1;

};

struct PartitionPointerHash {
  inline size_t operator()(const std::shared_ptr<Partition> &partition) const {
    return partition->hash();
  }
};

struct PartitionPointerPredicate {
  inline bool operator()(const std::shared_ptr<Partition>& lhs, const std::shared_ptr<Partition>& rhs) const {
    return lhs->equalTo(rhs);
  }
};

#endif //NORMAL_NORMAL_SQL_INCLUDE_NORMAL_SQL_CONNECTOR_PARTITION_PARTITION_H
