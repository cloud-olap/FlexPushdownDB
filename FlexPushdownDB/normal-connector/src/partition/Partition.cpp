//
// Created by matt on 15/4/20.
//

#include "normal/connector/partition/Partition.h"

const long &Partition::getNumBytes() const {
  return numBytes;
}

void Partition::setNumBytes(const long &NumBytes) {
  numBytes = NumBytes;
}
