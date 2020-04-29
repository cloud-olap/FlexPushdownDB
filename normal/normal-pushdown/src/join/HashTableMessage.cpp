//
// Created by matt on 29/4/20.
//

#include "normal/pushdown/join/HashTableMessage.h"

HashTableMessage::HashTableMessage(
	const std::shared_ptr<std::unordered_multimap<std::shared_ptr<arrow::Scalar>, long>> &hashTable,
	const std::string &sender) :
	Message("HashTableMessage", sender),
	hashtable_(hashTable) {
}

const std::shared_ptr<std::unordered_multimap<std::shared_ptr<arrow::Scalar>, long>>
&HashTableMessage::getHashtable() const {
  return hashtable_;
}
