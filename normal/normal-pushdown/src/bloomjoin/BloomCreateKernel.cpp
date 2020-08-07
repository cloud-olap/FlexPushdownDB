//
// Created by matt on 5/8/20.
//

#include "normal/pushdown/bloomjoin/BloomCreateKernel.h"

#include <utility>

BloomCreateKernel::BloomCreateKernel(std::string columnName,
									 double desiredFalsePositiveRate,
									 std::vector<std::string> bloomJoinUseSqlTemplates) :
	columnName_(std::move(columnName)),
	desiredFalsePositiveRate_(desiredFalsePositiveRate),
	bloomJoinUseSQLTemplates_(std::move(bloomJoinUseSqlTemplates)),
	receivedTupleSet_(std::nullopt),
	bloomFilter_(std::nullopt) {}

std::shared_ptr<BloomCreateKernel> BloomCreateKernel::make(const std::string &columnName,
														   double desiredFalsePositiveRate,
														   const std::vector<std::string> &bloomJoinUseSqlTemplates) {
  return std::make_shared<BloomCreateKernel>(columnName, desiredFalsePositiveRate, bloomJoinUseSqlTemplates);
}

tl::expected<void, std::string> BloomCreateKernel::addTupleSet(const std::shared_ptr<TupleSet2> &tupleSet) {
  if(!receivedTupleSet_.has_value()){
	receivedTupleSet_ = tupleSet;
	return {};
  }
  else{
	return receivedTupleSet_.value()->append(tupleSet);
  }
}

tl::expected<double, std::string> BloomCreateKernel::calculateBestFalsePositiveRate(const size_t &maxBloomJoinUseSQLTemplateSize) {

  if(!receivedTupleSet_.has_value()){
    return tl::make_unexpected("No TupleSets added");
  }

  ulong m = MaxS3SelectExpressionLength - (maxBloomJoinUseSQLTemplateSize + MaxBloomFilterPredicateSQLTemplateLength);
  auto requiredFalsePositiveRate = SlicedBloomFilter::calculateFalsePositiveRate(m, (int)receivedTupleSet_.value()->numRows());

  if (requiredFalsePositiveRate > desiredFalsePositiveRate_) {
	return requiredFalsePositiveRate;
  } else {
	return desiredFalsePositiveRate_;
  }
}

tl::expected<void, std::string> BloomCreateKernel::buildBloomFilter() {

  size_t maxBloomJoinUseSQLTemplateSize = 0;
  for(const auto &bloomJoinUseSQLTemplate: bloomJoinUseSQLTemplates_)
	maxBloomJoinUseSQLTemplateSize = std::max(maxBloomJoinUseSQLTemplateSize, bloomJoinUseSQLTemplate.size());

  auto expectedBestFalsePositiveRate = calculateBestFalsePositiveRate(maxBloomJoinUseSQLTemplateSize);
  if(!expectedBestFalsePositiveRate.has_value())
    return tl::make_unexpected(expectedBestFalsePositiveRate.error());

  bloomFilter_ = SlicedBloomFilter::make(std::max(1L, receivedTupleSet_.value()->numRows()), expectedBestFalsePositiveRate.value());

  int keyColumnIndex = receivedTupleSet_.value()->schema().value()->getFieldIndexByName(columnName_);
  auto table = receivedTupleSet_.value()->getArrowTable().value();

  ::arrow::TableBatchReader tableBatchReader(*table);
  auto result = tableBatchReader.Next();

  while(*result){
    auto recordBatch = *result;
	addRecordBatchToBloomFilter(*recordBatch, keyColumnIndex);
	result = tableBatchReader.Next();
  }

  return {};
}

const std::optional<std::shared_ptr<SlicedBloomFilter>> &BloomCreateKernel::getBloomFilter() const {
  return bloomFilter_;
}
