//
// Created by matt on 15/4/20.
//

#ifndef FPDB_FPDB_CATALOGUE_INCLUDE_FPDB_CATALOGUE_PARTITION_H
#define FPDB_FPDB_CATALOGUE_INCLUDE_FPDB_CATALOGUE_PARTITION_H

#include <fpdb/catalogue/Table.h>
#include <fpdb/tuple/Scalar.h>
#include <memory>
#include <string>
#include <optional>
#include <unordered_map>

using namespace fpdb::tuple;
using namespace std;

/**
 * Base class for partition meta data
 */
namespace fpdb::catalogue {

class Partition {

public:
  Partition() = default;
  Partition(const Partition&) = default;
  Partition& operator=(const Partition&) = default;

  virtual ~Partition() = default;

  virtual string toString() = 0;

  virtual bool equalTo(shared_ptr<Partition> other) = 0;

  virtual size_t hash() = 0;

  virtual CatalogueEntryType getCatalogueEntryType() = 0;

  const long &getNumBytes() const;
  const unordered_map<string, pair<shared_ptr<Scalar>, shared_ptr<Scalar>>> &getZoneMap() const;
  const weak_ptr<Table> &getTable() const;

  void setNumBytes(long numBytes);
  void addMinMax(const string &columnName,
                 const pair<shared_ptr<Scalar>, shared_ptr<Scalar>> &minMax);

protected:
  long numBytes_ = 0;
  unordered_map<string, pair<shared_ptr<Scalar>, shared_ptr<Scalar>>> zoneMap_;   // <columnName, <min, max>>
};

struct PartitionPointerHash {
  inline size_t operator()(const shared_ptr<Partition> &partition) const {
    return partition->hash();
  }
};

struct PartitionPointerPredicate {
  inline bool operator()(const shared_ptr<Partition> &lhs, const shared_ptr<Partition> &rhs) const {
    return lhs->equalTo(rhs);
  }
};

}

#endif //FPDB_FPDB_CATALOGUE_INCLUDE_FPDB_CATALOGUE_PARTITION_H
