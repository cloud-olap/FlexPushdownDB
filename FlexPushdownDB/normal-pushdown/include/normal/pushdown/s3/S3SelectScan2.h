//
// Created by matt on 14/8/20.
//

#ifndef NORMAL_NORMAL_PUSHDOWN_INCLUDE_NORMAL_PUSHDOWN_S3_S3SELECTSCAN2_H
#define NORMAL_NORMAL_PUSHDOWN_INCLUDE_NORMAL_PUSHDOWN_S3_S3SELECTSCAN2_H

#include <memory>
#include <string>

#include <aws/s3/S3Client.h>
#include <caf/all.hpp>

#include <normal/core/Operator.h>
#include <normal/core/OperatorActor2.h>
#include <normal/core/message/Envelope.h>
#include <normal/tuple/FileType.h>
#include <normal/core/message/CompleteMessage.h>
#include <normal/pushdown/scan/ScanOperator.h>
#include <normal/pushdown/scan/ScanMessage.h>
#include <normal/pushdown/s3/S3SelectCSVParseOptions.h>
#include <normal/pushdown/Forward.h>
#include "S3SelectScanKernel.h"

using namespace Aws::S3;
using namespace normal::core;
using namespace normal::core::message;
using namespace normal::pushdown::scan;
using namespace normal::tuple;

namespace normal::pushdown::s3 {

class S3SelectScan2 : public Operator {

public:

  S3SelectScan2(std::string name,
				const std::string &s3Bucket,
				const std::string &s3Object,
				const std::string &sql,
				std::optional<int64_t> startOffset,
				std::optional<int64_t> finishOffset,
				FileType fileType,
				std::optional<std::vector<std::string>> columnNames,
				const std::shared_ptr<arrow::Schema>& schema,
				const std::optional<S3SelectCSVParseOptions> &parseOptions,
				const std::shared_ptr<S3Client> &s3Client,
				bool scanOnStart,
				long queryId);

  static std::shared_ptr<S3SelectScan2> make(const std::string &name,
											 const std::string &s3Bucket,
											 const std::string &s3Object,
											 const std::string &sql,
											 std::optional<int64_t> startOffset,
											 std::optional<int64_t> finishOffset,
											 FileType fileType,
											 const std::optional<std::vector<std::string>> &columnNames,
											 const std::shared_ptr<arrow::Schema>& schema,
											 const std::optional<S3SelectCSVParseOptions> &parseOptions,
											 const std::shared_ptr<S3Client> &s3Client,
											 bool scanOnStart,
											 long queryId = 0);

  void onReceive(const Envelope &message) override;

private:
  // Column names to scan when scanOnStart_ is true
  std::optional<std::vector<std::string>> columnNames_;
  bool scanOnStart_;
  std::unique_ptr<S3SelectScanKernel> kernel_;
  // Column names to scan when receiving a scanmessage
  std::optional<std::vector<std::string>> columnNamesToScan_;

  [[nodiscard]] tl::expected<void, std::string> onStart();
  void onComplete(const CompleteMessage &message);
  void onScan(const ScanMessage &Message);

  void requestStoreSegmentsInCache(const std::shared_ptr<TupleSet2> &tupleSet);
  void readAndSendTuples(const std::vector<std::string> &columnNames);
};

using GetMetricsAtom = caf::atom_constant<caf::atom("g-metrics")>;

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

#endif //NORMAL_NORMAL_PUSHDOWN_INCLUDE_NORMAL_PUSHDOWN_S3_S3SELECTSCAN2_H
