//
// Created by matt on 16/12/19.
//

#ifndef NORMAL_NORMAL_CORE_SRC_NORMAL_H
#define NORMAL_NORMAL_CORE_SRC_NORMAL_H

#include <memory>

#include <normal/core/OperatorManager.h>
#include <normal/core/graph/OperatorGraph.h>
#include <normal/core/Forward.h>

using namespace normal::core::graph;

namespace normal::core {

/**
 * Placeholder for an eventual API
 */
class Normal {

public:
  Normal();

  static std::shared_ptr<Normal> start();
  void stop();

  std::shared_ptr<OperatorGraph> createQuery();

private:
  std::shared_ptr<OperatorManager> operatorManager_;

};

}

#endif //NORMAL_NORMAL_CORE_SRC_NORMAL_H
