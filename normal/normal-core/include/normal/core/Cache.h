//
// Created by Jialing on 20/01/19.
//

#ifndef NORMAL_NORMAL_NORMAL_CORE_SRC_CACHE_H
#define NORMAL_NORMAL_NORMAL_CORE_SRC_CACHE_H

#include <memory>
#include <string>
#include <unordered_map> 

#include "normal/core/Operator.h"
#include "normal/tuple/TupleSet.h"

class Cache {
private:
 
  


public:
	//name/id->tupleSets
  std::unordered_map<std::string, std::shared_ptr<normal::core::TupleSet>> m_cacheData;

  ~Cache() = default;
};

#endif //NORMAL_NORMAL_NORMAL_CORE_SRC_CACHE_H
