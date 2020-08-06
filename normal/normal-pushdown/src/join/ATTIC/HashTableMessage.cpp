//
// Created by matt on 29/4/20.
//

#include "normal/pushdown/join/ATTIC/HashTableMessage.h"

using namespace normal::pushdown;
using namespace normal::pushdown::join;

HashTableMessage::HashTableMessage(const std::shared_ptr<HashTable> &hashTable, const std::string &sender) :
	Message("HashTableMessage", sender),
	hashtable_(hashTable) {
}

const std::shared_ptr<HashTable>
&HashTableMessage::getHashtable() const {
  return hashtable_;
}
