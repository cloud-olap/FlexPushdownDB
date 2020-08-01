//
// Created by matt on 1/8/20.
//

#include "ArraySetIndex.h"

#include <utility>
ArraySetIndex::ArraySetIndex(size_t ArrayPos,
							 std::shared_ptr<::arrow::Table> Table) : table_(std::move(Table)), arrayPos_(ArrayPos) {}
