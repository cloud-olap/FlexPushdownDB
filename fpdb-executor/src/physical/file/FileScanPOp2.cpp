//
// Created by matt on 23/9/20.
//

#include <fpdb/executor/physical/file/FileScanPOp2.h>
#include <utility>

//namespace fpdb::executor::physical::file {
//
//FileScanActor::behavior_type FileScanFunctor(FileScanStatefulActor self,
//                       const std::string &name,
//                       const std::string &bucket,
//                       const std::string &object,
//                       const std::string &storeRootPath,
//                       const std::shared_ptr<FileFormat> &format,
//                       const std::shared_ptr<::arrow::Schema> &schema,
//                       const std::vector<std::string> &columnNames,
//                       long queryId,
//                       const ::caf::actor &rootActorHandle,
//                       const ::caf::actor &segmentCacheActorHandle,
//                       const std::optional<std::pair<int64_t, int64_t>> &byteRange,
//                       bool scanOnStart) {
//
//  self->state.setState(self,
//					   name,
//					   bucket,
//             object,
//             storeRootPath,
//             format,
//             schema,
//					   columnNames,
//					   queryId,
//					   rootActorHandle,
//					   segmentCacheActorHandle,
//             byteRange,
//					   scanOnStart);
//
//  return self->state.makeBehavior(self);
//}
//
//}
