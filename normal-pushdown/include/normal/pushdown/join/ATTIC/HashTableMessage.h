//
// Created by matt on 29/4/20.
//

#ifndef NORMAL_NORMAL_PUSHDOWN_INCLUDE_NORMAL_PUSHDOWN_JOIN_HASHTABLEMESSAGE_H
#define NORMAL_NORMAL_PUSHDOWN_INCLUDE_NORMAL_PUSHDOWN_JOIN_HASHTABLEMESSAGE_H

#include <memory>
#include <unordered_map>

#include <arrow/scalar.h>

#include <normal/core/message/Message.h>
#include "normal/pushdown/join/ATTIC/HashTable.h"

namespace normal::pushdown::join {

class HashTableMessage : public normal::core::message::Message {

public:
  HashTableMessage(const std::shared_ptr<HashTable> &hashTable,
				   const std::string &sender);

  const std::shared_ptr<HashTable> &getHashtable() const;

private:
  std::shared_ptr<HashTable> hashtable_;

};

}

#endif //NORMAL_NORMAL_PUSHDOWN_INCLUDE_NORMAL_PUSHDOWN_JOIN_HASHTABLEMESSAGE_H
