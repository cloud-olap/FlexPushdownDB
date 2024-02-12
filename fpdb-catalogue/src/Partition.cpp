//
// Created by matt on 15/4/20.
//

#include <fpdb/catalogue/Partition.h>

namespace fpdb::catalogue {

const long &Partition::getNumBytes() const {
  return numBytes_;
}

const unordered_map<string, pair<shared_ptr<Scalar>, shared_ptr<Scalar>>> &Partition::getZoneMap() const {
  return zoneMap_;
}

void Partition::setNumBytes(long numBytes) {
  numBytes_ = numBytes;
}

void Partition::addMinMax(const string &columnName,
                          const pair<shared_ptr<Scalar>, shared_ptr<Scalar>> &minMax) {
  zoneMap_.emplace(columnName, minMax);
}

}
