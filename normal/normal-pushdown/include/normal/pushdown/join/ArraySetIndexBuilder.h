//
// Created by matt on 1/8/20.
//

#ifndef NORMAL_NORMAL_PUSHDOWN_INCLUDE_NORMAL_PUSHDOWN_JOIN_ARRAYSETINDEXBUILDER_H
#define NORMAL_NORMAL_PUSHDOWN_INCLUDE_NORMAL_PUSHDOWN_JOIN_ARRAYSETINDEXBUILDER_H

#include <memory>
#include "ArraySetIndex.h"
#include "TypedArraySetIndex.h"

class ArraySetIndexBuilder {
public:
  static std::shared_ptr<ArraySetIndex> make(const std::shared_ptr<::arrow::Table>& table, const std::string& arrayName){
	size_t arrayPos = table->schema()->GetFieldIndex(arrayName);
	return TypedArraySetIndex<std::string, ::arrow::StringType>::make(table, arrayPos);
  }
};

#endif //NORMAL_NORMAL_PUSHDOWN_INCLUDE_NORMAL_PUSHDOWN_JOIN_ARRAYSETINDEXBUILDER_H
