//
// Created by matt on 15/9/20.
//
// Forward declarations

#ifndef FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_FORWARD_H
#define FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_FORWARD_H

namespace fpdb::executor {

namespace physical {
//  class OperatorManager;
  class PhysicalOp;
  class POpActor;
  class POpContext;

  namespace collate {
    class CollateState;
  }

  namespace file {
    class FileScanState;
  }

  namespace s3 {
    class [[maybe_unused]] S3SelectScanState;
  }
}

namespace cache {
  class SegmentCacheActor;
}

//namespace graph {
//  class OperatorGraph;
//}

namespace message {
  class Envelope;
}

}

#endif //FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_FORWARD_H
