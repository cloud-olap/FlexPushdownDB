//
// Created by matt on 4/12/19.
//

#include <string>
#include <memory>
#include <vector>

#define DOCTEST_CONFIG_IMPLEMENT
#include <doctest/doctest.h>

#include <arrow/array/builder_binary.h>           // for StringBuilder
#include <arrow/table.h>                          // for Table
#include <arrow/type.h>                           // for field, schema, Schema
#include <arrow/type_fwd.h>                       // for default_memory_pool
#include <memory.h>                      // for shared_ptr, make_sh...
#include <cstdio>                                // for FILENAME_MAX
#include <unistd.h>                               // for getcwd
#include <iostream>                               // for cout

#include "normal/pushdown/S3SelectScan.h"
#include "normal/pushdown/Collate.h"
#include "normal/core/OperatorContext.h"
#include <normal/core/OperatorManager.h>
#include <normal/pushdown/Aggregate.h>
#include <normal/pushdown/FileScan.h>
#include <normal/core/Normal.h>
#include "normal/core/TupleSet.h"                 // for TupleSet
#include "normal/pushdown/AggregateExpression.h"  // for AggregateExpression

#include "Globals.h"

namespace arrow { class Array; }
namespace arrow { class MemoryPool; }
namespace arrow { class StringArray; }

namespace normal::test {}

int main(int argc, char **argv) {

  spdlog::set_level(spdlog::level::debug);
  spdlog::set_pattern("[%H:%M:%S.%e] [thread %t] [%! (%s:%#)] [%l]  %v");

  return doctest::Context(argc, argv).run();
}
