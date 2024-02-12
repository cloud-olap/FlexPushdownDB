//
// Created by matt on 14/8/20.
//

#ifndef FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_S3_S3SELECTSCANPOP2_H
#define FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_S3_S3SELECTSCANPOP2_H

//#include <fpdb/executor/physical/s3/S3SelectScanKernel.h>
//#include <fpdb/executor/physical/s3/S3SelectCSVParseOptions.h>
//#include <fpdb/executor/physical/Forward.h>
//#include <fpdb/executor/physical/PhysicalOp.h>
//#include <fpdb/executor/physical/POpActor2.h>
//#include <fpdb/executor/message/Envelope.h>
//#include <fpdb/executor/message/CompleteMessage.h>
//#include <fpdb/executor/message/ScanMessage.h>
//#include <fpdb/catalogue/Table.h>
//#include <fpdb/aws/AWSClient.h>
//#include <fpdb/tuple/FileType.h>
//#include <memory>
//#include <string>
//#include <aws/s3/S3Client.h>
//#include <caf/all.hpp>
//
//using namespace fpdb::executor::message;
//using namespace fpdb::catalogue;
//using namespace fpdb::aws;
//using namespace fpdb::tuple;
//
//namespace fpdb::executor::physical::s3 {
//
//class S3SelectScanPOp2 : public PhysicalOp {
//
//public:
//
//  S3SelectScanPOp2(std::string name,
//				const std::string &s3Bucket,
//				const std::string &s3Object,
//				const std::string &sql,
//				std::optional<int64_t> startOffset,
//				std::optional<int64_t> finishOffset,
//				FileType fileType,
//        const std::optional<S3SelectCSVParseOptions> &parseOptions,
//				std::optional<std::vector<std::string>> columnNames,
//				const std::shared_ptr<Table>& table,
//				const std::shared_ptr<AWSClient> &awsClient,
//				bool scanOnStart,
//				long queryId);
//
//  static std::shared_ptr<S3SelectScanPOp2> make(const std::string &name,
//											 const std::string &s3Bucket,
//											 const std::string &s3Object,
//											 const std::string &sql,
//											 std::optional<int64_t> startOffset,
//											 std::optional<int64_t> finishOffset,
//											 FileType fileType,
//                       const std::optional<S3SelectCSVParseOptions> &parseOptions,
//											 const std::optional<std::vector<std::string>> &columnNames,
//											 const std::shared_ptr<Table>& table,
//											 const std::shared_ptr<AWSClient> &awsClient,
//											 bool scanOnStart,
//											 long queryId = 0);
//
//  void onReceive(const Envelope &message) override;
//
//private:
//  // Column names to scan when scanOnStart_ is true
//  std::optional<std::vector<std::string>> columnNames_;
//  bool scanOnStart_;
//  std::unique_ptr<S3SelectScanKernel> kernel_;
//  // Column names to scan when receiving a scanmessage
//  std::optional<std::vector<std::string>> columnNamesToScan_;
//
//  [[nodiscard]] tl::expected<void, std::string> onStart();
//  void onComplete(const CompleteMessage &message);
//  void onScan(const ScanMessage &Message);
//
//  void requestStoreSegmentsInCache(const std::shared_ptr<TupleSet2> &tupleSet);
//  void readAndSendTuples(const std::vector<std::string> &columnNames);
//};
//
//using GetMetricsAtom = caf::atom_constant<caf::atom("g-metrics")>;

//using S3SelectScanActor = OperatorActor2::extend_with<::caf::typed_actor<
//	caf::reacts_to<ScanAtom, std::vector<std::string>, bool>>>;
//
//using S3SelectScanStatefulActor = S3SelectScanActor::stateful_pointer<S3SelectScanState>;
//
//class S3SelectScanState : public OperatorActorState<S3SelectScanStatefulActor> {
//public:
//  void setState(S3SelectScanStatefulActor self,
//				const char *name_,
//				const std::string &s3Bucket,
//				const std::string &s3Object,
//				const std::string &sql,
//				std::optional<int64_t> startOffset,
//				std::optional<int64_t> finishOffset,
//				FileType fileType,
//				const std::optional<std::vector<std::string>> &columnNames,
//				const std::optional<S3SelectCSVParseOptions> &parseOptions,
//				const std::shared_ptr<S3Client> &s3Client,
//				bool scanOnStart) {
//
//	OperatorActorState::setBaseState(self, name_);
//
//	legacyOperator_ = S3SelectScan2::make(name_,
//										  s3Bucket,
//										  s3Object,
//										  sql,
//										  startOffset,
//										  finishOffset,
//										  fileType,
//										  columnNames,
//										  parseOptions,
//										  s3Client,
//										  scanOnStart);
//  }
//
//  template<class... Handlers>
//  S3SelectScanActor::behavior_type makeBehavior(S3SelectScanStatefulActor self, Handlers... handlers) {
//	return OperatorActorState::makeBaseBehavior(
//		self,
//		[=](ScanAtom, const std::vector<std::string> &columnNames, bool resultNeeded) {
//		  SPDLOG_DEBUG("[Actor {} ('{}')]  Scan  |  sender: {}", self->id(),
//					   self->name(), to_string(self->current_sender()));
//		  self->state.legacyOperator_->onReceive(Envelope(std::make_shared<ScanMessage>(columnNames,
//																						to_string(self->current_sender()),
//																						resultNeeded)));
//		},
//		std::move(handlers)...
//	);
//  }
//
//  static S3SelectScanActor::behavior_type spawnFunctor(S3SelectScanStatefulActor self,
//													   const char *name_,
//													   const std::string &s3Bucket,
//													   const std::string &s3Object,
//													   const std::string &sql,
//													   std::optional<int64_t> startOffset,
//													   std::optional<int64_t> finishOffset,
//													   FileType fileType,
//													   const std::optional<std::vector<std::string>> &columnNames,
//													   const std::optional<S3SelectCSVParseOptions> &parseOptions,
//													   const std::shared_ptr<S3Client> &s3Client,
//													   bool scanOnStart) {
//	self->state.setState(self,
//						 name_,
//						 s3Bucket,
//						 s3Object,
//						 sql,
//						 startOffset,
//						 finishOffset,
//						 fileType,
//						 columnNames,
//						 parseOptions,
//						 s3Client,
//						 scanOnStart);
//
//	return self->state.makeBehavior(self);
//  }
//
//private:
//  std::shared_ptr<S3SelectScan2> legacyOperator_;
//
//};

}

#endif //FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_S3_S3SELECTSCANPOP2_H
