//
// Created by matt on 5/8/20.
//

#ifndef NORMAL_NORMAL_PUSHDOWN_INCLUDE_NORMAL_PUSHDOWN_BLOOMJOIN_FILESCANBLOOMUSEKERNEL_H
#define NORMAL_NORMAL_PUSHDOWN_INCLUDE_NORMAL_PUSHDOWN_BLOOMJOIN_FILESCANBLOOMUSEKERNEL_H

#include <utility>

#include <normal/tuple/csv/CSVParser.h>
#include <normal/tuple/ArrayAppender.h>
#include <normal/tuple/TupleSet2.h>
#include <normal/pushdown/bloomjoin/SlicedBloomFilter.h>

using namespace normal::tuple;
using namespace normal::tuple::csv;

class FileScanBloomUseKernel {

public:
  FileScanBloomUseKernel(std::string filePath,
						 std::vector<std::string> columnNames,
						 unsigned long startOffset,
						 unsigned long finishOffset,
						 std::string bloomFilterColumnName) :
	  filePath_(std::move(filePath)),
	  columnNames_(std::move(columnNames)),
	  startOffset_(startOffset),
	  finishOffset_(finishOffset),
	  bloomFilterColumnName_(std::move(bloomFilterColumnName)),
	  parser_(filePath_, columnNames_, startOffset_, finishOffset_) {
  }

  static std::shared_ptr<FileScanBloomUseKernel> make(const std::string &filePath,
													  const std::vector<std::string> &columnNames,
													  unsigned long startOffset,
													  unsigned long finishOffset,
													  const std::string& bloomFilterColumnName) {
	return std::make_shared<FileScanBloomUseKernel>(filePath,
													columnNames,
													startOffset,
													finishOffset,
													bloomFilterColumnName);
  }

  [[nodiscard]]  tl::expected<void, std::string> setBloomFilter(const std::shared_ptr<SlicedBloomFilter> &bloomFilter) {

	if (bloomFilter_)
	  return tl::make_unexpected("Bloom filter already set");

	bloomFilter_ = bloomFilter;
	return {};
  }

  [[nodiscard]] tl::expected<void, std::string> scan() {

	auto expectedTupleSet = parser_.parse();
	if (!expectedTupleSet)
	  return tl::make_unexpected(expectedTupleSet.error());

	tupleSet_ = expectedTupleSet.value();
	return {};
  }

  size_t size() {
	if (!tupleSet_)
	  return 0;
	else
	  return tupleSet_.value()->numRows();
  }

  [[nodiscard]] tl::expected<void, std::string> filter() {

	auto table = tupleSet_.value()->getArrowTable().value();
	auto filterColumnIndex = table->schema()->GetFieldIndex(bloomFilterColumnName_);

	std::vector<std::shared_ptr<ArrayAppender>> appenders(table->num_columns());
	for (int c = 0; c < table->num_columns(); ++c) {
	  auto expectedAppender = ArrayAppender::make(table->column(c)->type(), 0);
	  if (!expectedAppender.has_value())
		return tl::make_unexpected(expectedAppender.error());
	  appenders[c] = expectedAppender.value();
	}

	::arrow::TableBatchReader reader(*table);
	auto result = reader.Next();

	while (*result) {

	  auto recordBatch = *result;

	  std::vector<std::shared_ptr<::arrow::Array>> columns(recordBatch->num_columns());
	  for (int c = 0; c < recordBatch->num_columns(); ++c) {
		columns[c] = recordBatch->column(c);
	  }

	  auto columnIntArray = std::static_pointer_cast<::arrow::Int64Array>(recordBatch->column(filterColumnIndex));

	  for (int r = 0; r < recordBatch->num_rows(); ++r) {
		if (bloomFilter_.value()->contains(columnIntArray->Value(r))) {
		  for (int c = 0; c < table->num_columns(); ++c) {
			appenders[c]->appendValue(columns[c], r);
		  }
		}
	  }

	  result = reader.Next();
	}

	::arrow::ArrayVector filteredArrayVector_(table->schema()->num_fields());

	for (size_t c = 0; c < appenders.size(); ++c) {
	  auto expectedArray = appenders[c]->finalize();
	  if (!expectedArray.has_value())
		return tl::make_unexpected(expectedArray.error());
	  filteredArrayVector_[c] = expectedArray.value();
	}

	auto filteredTable = ::arrow::Table::Make(table->schema(), filteredArrayVector_);
	tupleSet_ = TupleSet2::make(filteredTable);

	return {};
  }

  [[nodiscard]] const std::optional<std::shared_ptr<TupleSet2>> &getTupleSet() const {
	return tupleSet_;
  }

private:
  std::string filePath_;
  std::vector<std::string> columnNames_;
  unsigned long startOffset_;
  unsigned long finishOffset_;
  std::string bloomFilterColumnName_;

  CSVParser parser_;

  std::optional<std::shared_ptr<SlicedBloomFilter>> bloomFilter_;

  std::optional<std::shared_ptr<TupleSet2>> tupleSet_;

};

#endif //NORMAL_NORMAL_PUSHDOWN_INCLUDE_NORMAL_PUSHDOWN_BLOOMJOIN_FILESCANBLOOMUSEKERNEL_H
