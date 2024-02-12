//
// Created by matt on 23/9/20.
//

#ifndef FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_FILE_FILESCANPOP2_H
#define FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_FILE_FILESCANPOP2_H

#include <fpdb/executor/physical/Forward.h>
#include <fpdb/executor/physical/file/FileScanAbstractPOp.h>
#include <fpdb/executor/physical/POpActor2.h>
#include <fpdb/executor/cache/SegmentCacheActor.h>
#include <fpdb/executor/message/TupleSetMessage.h>
#include <fpdb/executor/message/cache/StoreRequestMessage.h>
#include <fpdb/executor/caf-serialization/CAFMessageSerializer.h>
#include <fpdb/catalogue/local-fs/LocalFSPartition.h>
#include <fpdb/caf/CAFUtil.h>
#include <utility>

using namespace fpdb::executor::message;
using namespace fpdb::executor::cache;
using namespace fpdb::tuple;

CAF_BEGIN_TYPE_ID_BLOCK(FileScanPOp2, fpdb::caf::CAFUtil::FileScanPOp2_first_custom_type_id)
CAF_ADD_ATOM(FileScanPOp2, ScanAtom)
CAF_ADD_TYPE_ID(FileScanPOp2, (std::vector<std::string>))
CAF_END_TYPE_ID_BLOCK(FileScanPOp2)

// TODO: this class is out-of-date
//namespace fpdb::executor::physical::file {
//
//using FileScanActor = POpActor2::extend_with<::caf::typed_actor<
//	::caf::reacts_to<ScanAtom, std::vector<std::string>, bool>>>;
//
//using FileScanStatefulActor = FileScanActor::stateful_pointer<FileScanState>;
//
//class FileScanState : public POpActorState<FileScanStatefulActor> {
//public:
//  void setState(FileScanStatefulActor actor,
//				const std::string &name,
//        const std::string &bucket,
//        const std::string &object,
//        const std::string &storeRootPath,
//        const std::shared_ptr<FileFormat> &format,
//        const std::shared_ptr<::arrow::Schema> &schema,
//				const std::vector<std::string> &columnNames,
//				long queryId,
//				const ::caf::actor &rootActorHandle,
//				const ::caf::actor &segmentCacheActorHandle,
//        const std::optional<std::pair<int64_t, int64_t>> &byteRange = std::nullopt,
//				bool scanOnStart = false) {
//
//	POpActorState::setBaseState(actor, std::move(name), queryId, rootActorHandle, segmentCacheActorHandle);
//
//	auto canonicalColumnNames = ColumnName::canonicalize(columnNames);
//
//	columnNames_ = canonicalColumnNames;
//	scanOnStart_ = scanOnStart;
//
//	kernel_ = FileScanKernel::make(bucket, object, storeRootPath, format, schema, byteRange);
//  }
//
//  template<class... Handlers>
//  FileScanActor::behavior_type makeBehavior(FileScanStatefulActor actor, Handlers... handlers) {
//	return POpActorState::makeBaseBehavior(
//		actor,
//		[=](ScanAtom, const std::vector<std::string> &columnNames, bool /*resultNeeded*/) {
//		  process(actor,
//				  [=](const ::caf::strong_actor_ptr &messageSender) {
//					return onScan(actor,
//								  messageSender,
//								  columnNames);
//				  });
//		},
//		std::move(handlers)...
//	);
//  }
//
//private:
//  std::vector<std::string> columnNames_;
//  bool scanOnStart_;
//
//  FileScanKernel kernel_;
//
//protected:
//
//  tl::expected<void, std::string>
//  onStart(FileScanStatefulActor actor, const ::caf::strong_actor_ptr & /*messageSender*/) override {
//	if (scanOnStart_) {
//	  return readAndSendTuples(actor, columnNames_)
//		  .and_then([=]() { return notifyComplete(actor); });
//	}
//	return {};
//  }
//
//  tl::expected<void, std::string>
//  onComplete(FileScanStatefulActor actor, const ::caf::strong_actor_ptr & /*messageSender*/) override {
//	if (!isComplete() && isAllProducersComplete()) {
//	  return notifyComplete(actor);
//	}
//	return {};
//  }
//
//  tl::expected<void, std::string>
//  onEnvelope(FileScanStatefulActor actor,
//			 const ::caf::strong_actor_ptr &messageSender,
//			 const Envelope &envelope) override {
//	if (envelope.message().type() == MessageType::SCAN) {
//	  auto scanMessage = dynamic_cast<const ScanMessage &>(envelope.message());
//	  return this->onScan(actor, messageSender, scanMessage.getColumnNames());
//	} else {
//	  return tl::make_unexpected(fmt::format("Unrecognized message type {}", envelope.message().getTypeString()));
//	}
//  }
//
//private:
//
//  [[nodiscard]] tl::expected<void, std::string>
//  onScan(FileScanStatefulActor actor,
//		 const ::caf::strong_actor_ptr &messageSender,
//		 const std::vector<std::string> &columnsToScan) {
//
//	SPDLOG_DEBUG("[Actor {} ('{}')]  Scan  |  sender: {}", actor->id(),
//				 actor->name(), to_string(messageSender));
//
//	return readAndSendTuples(actor, columnsToScan);
//  }
//
//  tl::expected<void, std::string> requestStoreSegmentsInCache(FileScanStatefulActor actor,
//                                                              const std::shared_ptr<TupleSet> &tupleSet) {
//
//	assert(tupleSet);
//
//	auto partition = std::make_shared<catalogue::local_fs::LocalFSPartition>(kernel_.getFilePath());
//  std::pair<int64_t, int64_t> byteRange;
//  auto optByteRange = kernel_.getByteRange();
//  if (optByteRange.has_value()) {
//    byteRange = *optByteRange;
//  } else {
//    auto expFileSize = kernel_.getFileSize();
//    if (!expFileSize.has_value()) {
//      return tl::make_unexpected(expFileSize.error());
//    }
//    byteRange = {0, *expFileSize};
//  }
//
//	std::unordered_map<std::shared_ptr<SegmentKey>, std::shared_ptr<SegmentData>> segmentsToStore;
//	for (int64_t c = 0; c < tupleSet->numColumns(); ++c) {
//	  auto column = tupleSet->getColumnByIndex(c).value();
//	  auto segmentKey = SegmentKey::make(partition,
//										 column->getName(),
//										 SegmentRange::make(byteRange.first, byteRange.second),
//										 SegmentMetadata::make(column->size()));
//	  auto segmentData = SegmentData::make(column);
//
//	  segmentsToStore.emplace(segmentKey, segmentData);
//	}
//
//	anonymousSend(actor,
//				  getSegmentCacheActorHandle().value(),
//				  StoreAtom_v,
//				  StoreRequestMessage::make(segmentsToStore, name));
//
//  return {};
//  }
//
//  [[nodiscard]] tl::expected<void, std::string>
//  readAndSendTuples(FileScanStatefulActor actor, const std::vector<std::string> &columnNames) {
//	// Read the columns not present in the cache
//	/*
//	 * FIXME: Should support reading the file in pieces
//	 */
//
//	std::shared_ptr<TupleSet> readTupleSet;
//	if (columnNames.empty()) {
//	  readTupleSet = TupleSet::makeWithEmptyTable();
//	} else {
//	  auto expectedReadTupleSet = kernel_.scan(columnNames);
//	  readTupleSet = expectedReadTupleSet.value();
//
//	  // Store the read columns in the cache
//	  auto result = requestStoreSegmentsInCache(actor, readTupleSet);
//    if (!result.has_value()) {
//      return result;
//    }
//	}
//
//	std::shared_ptr<Message> message = std::make_shared<TupleMessage>(readTupleSet, this->name);
//	return tell(actor, Envelope(message));
//  }
//
//};
//
//FileScanActor::behavior_type FileScanFunctor(FileScanStatefulActor actor,
//                       const std::string &name,
//                       const std::string &bucket,
//                       const std::string &object,
//                       const std::string &storeRootPath,
//                       const std::shared_ptr<FileFormat> &format,
//                       const std::shared_ptr<::arrow::Schema> &schema,
//											 const std::vector<std::string> &columnNames,
//											 unsigned long startOffset,
//											 unsigned long finishOffset,
//											 long queryId,
//											 const ::caf::actor &rootActorHandle,
//											 const ::caf::actor &segmentCacheActorHandle,
//                       const std::optional<std::pair<int64_t, int64_t>> &byteRange = std::nullopt,
//											 bool scanOnStart = false);
//
//}

#endif //FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_FILE_FILESCANPOP2_H
