//
// Created by Yifei Yang on 1/14/22.
//

#include <fpdb/executor/caf/CAFInit.h>
#include <fpdb/executor/physical/POpActor.h>
#include <fpdb/executor/physical/POpActor2.h>
#include <fpdb/executor/physical/collate/CollatePOp2.h>
#include <fpdb/executor/physical/file/FileScanPOp2.h>
#include <fpdb/executor/physical/PhysicalPlan.h>
#include <fpdb/executor/cache/SegmentCacheActor.h>
#include <fpdb/executor/message/Envelope.h>
#include <fpdb/executor/caf-serialization/CAFMessageSerializer.h>
#include <fpdb/executor/caf-serialization/CAFPOpSerializer.h>
#include <fpdb/executor/caf-serialization/CAFAggregateFunctionSerializer.h>
#include <fpdb/executor/caf-serialization/CAFHashJoinProbeAbstractKernelSerializer.h>
#include <fpdb/executor/caf-serialization/CAFFileScanKernelSerializer.h>
#include <fpdb/executor/caf-serialization/CAFGroupAbstractKernelSerializer.h>
#include <fpdb/executor/caf-serialization/CAFBloomFilterCreateAbstractKernelSerializer.h>
#include <fpdb/executor/caf-serialization/CAFBloomFilterSerializer.h>
#include <fpdb/cache/caf-serialization/CAFCachingPolicySerializer.h>
#include <fpdb/catalogue/caf-serialization/CAFTableSerializer.h>
#include <fpdb/catalogue/caf-serialization/CAFObjStoreConnectorSerializer.h>
#include <fpdb/expression/gandiva/caf-serialization/CAFExpressionSerializer.h>
#include <fpdb/tuple/caf-serialization/CAFFileFormatSerializer.h>

namespace fpdb::executor::caf {

void CAFInit::initCAFGlobalMetaObjects() {
  ::caf::init_global_meta_objects<::caf::id_block::SegmentCacheActor>();
  ::caf::init_global_meta_objects<::caf::id_block::Envelope>();
  ::caf::init_global_meta_objects<::caf::id_block::POpActor>();
  ::caf::init_global_meta_objects<::caf::id_block::POpActor2>();
  ::caf::init_global_meta_objects<::caf::id_block::CollatePOp2>();
  ::caf::init_global_meta_objects<::caf::id_block::FileScanPOp2>();
  ::caf::init_global_meta_objects<::caf::id_block::TupleSet>();
  ::caf::init_global_meta_objects<::caf::id_block::Message>();
  ::caf::init_global_meta_objects<::caf::id_block::SegmentKey>();
  ::caf::init_global_meta_objects<::caf::id_block::Partition>();
  ::caf::init_global_meta_objects<::caf::id_block::SegmentMetadata>();
  ::caf::init_global_meta_objects<::caf::id_block::SegmentData>();
  ::caf::init_global_meta_objects<::caf::id_block::Column>();
  ::caf::init_global_meta_objects<::caf::id_block::TupleSetIndex>();
  ::caf::init_global_meta_objects<::caf::id_block::TupleKey>();
  ::caf::init_global_meta_objects<::caf::id_block::TupleKeyElement>();
  ::caf::init_global_meta_objects<::caf::id_block::POp>();
  ::caf::init_global_meta_objects<::caf::id_block::Scalar>();
  ::caf::init_global_meta_objects<::caf::id_block::AggregateFunction>();
  ::caf::init_global_meta_objects<::caf::id_block::Expression>();
  ::caf::init_global_meta_objects<::caf::id_block::POpContext>();
  ::caf::init_global_meta_objects<::caf::id_block::Table>();
  ::caf::init_global_meta_objects<::caf::id_block::FileFormat>();
  ::caf::init_global_meta_objects<::caf::id_block::HashJoinProbeAbstractKernel>();
  ::caf::init_global_meta_objects<::caf::id_block::AggregateResult>();
  ::caf::init_global_meta_objects<::caf::id_block::FileScanKernel>();
  ::caf::init_global_meta_objects<::caf::id_block::PhysicalPlan>();
  ::caf::init_global_meta_objects<::caf::id_block::BloomFilter>();
  ::caf::init_global_meta_objects<::caf::id_block::ObjStoreConnector>();
  ::caf::init_global_meta_objects<::caf::id_block::GroupAbstractKernel>();
  ::caf::init_global_meta_objects<::caf::id_block::FPDBStoreBloomFilterUseInfo>();
  ::caf::init_global_meta_objects<::caf::id_block::Mode>();
  ::caf::init_global_meta_objects<::caf::id_block::CachingPolicy>();
  ::caf::init_global_meta_objects<::caf::id_block::BloomFilterCreateAbstractKernel>();

  ::caf::core::init_global_meta_objects();
  ::caf::io::middleman::init_global_meta_objects();
}

}
