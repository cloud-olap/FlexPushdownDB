//
// Created by matt on 20/7/20.
//

#include "normal/pushdown/merge/MergeKernel.h"

using namespace normal::pushdown::merge;

std::optional<std::string> MergeKernel::validateTupleSets(const std::shared_ptr<TupleSet2> &tupleSet1,
														  const std::shared_ptr<TupleSet2> &tupleSet2) {

  // Check row count is equal
  if (tupleSet1->numRows() != tupleSet2->numRows()) {
	return fmt::format(
		"Cannot merge TupleSets, number of rows must be equal. "
		"tupleSet1.numRows: {} != tupleSet2.numRows: {}",
		tupleSet1->numRows(), tupleSet2->numRows());
  }

  // Check field names do not contain duplicates
  if (tupleSet1->getArrowTable().has_value() && tupleSet2->getArrowTable().has_value()) {
	for (const auto &field1: tupleSet1->getArrowTable().value()->schema()->fields()) {
	  for (const auto &field2: tupleSet2->getArrowTable().value()->schema()->fields()) {
		if (field1->name() == field2->name()) {
		  return fmt::format(
			  "Cannot merge TupleSets, field names must not contain duplicates. "
			  "Multiple fields with name: {}",
			  field1->name());
		}
	  }
	}
  }

  return std::nullopt;
}

std::shared_ptr<Schema> MergeKernel::mergeSchema(const std::shared_ptr<TupleSet2> &tupleSet1,
												 const std::shared_ptr<TupleSet2> &tupleSet2) {

  if (tupleSet1->getArrowTable().has_value() && tupleSet2->getArrowTable().has_value()) {
	const auto &fields1 = tupleSet1->getArrowTable().value()->schema()->fields();
	const auto &fields2 = tupleSet2->getArrowTable().value()->schema()->fields();
	auto mergedFields = std::vector<std::shared_ptr<arrow::Field>>{};
	mergedFields.reserve(fields1.size() + fields2.size());
	mergedFields.insert(std::end(mergedFields), std::begin(fields1), std::end(fields1));
	mergedFields.insert(std::end(mergedFields), std::begin(fields2), std::end(fields2));

	const auto &mergedArrowSchema = std::make_shared<::arrow::Schema>(mergedFields);
	const auto &mergedSchema = Schema::make(mergedArrowSchema);
	return mergedSchema;
  } else {
	// Handle empty tupleset
	const auto & mergedFields = std::vector<std::shared_ptr<arrow::Field>>{};
	const auto &mergedArrowSchema = std::make_shared<::arrow::Schema>(mergedFields);
	const auto &mergedSchema = Schema::make(mergedArrowSchema);
	return mergedSchema;
  }
}

std::vector<std::shared_ptr<::arrow::ChunkedArray>> MergeKernel::mergeArrays(const std::shared_ptr<TupleSet2> &tupleSet1,
																			 const std::shared_ptr<TupleSet2> &tupleSet2) {
  if (tupleSet1->getArrowTable().has_value() && tupleSet2->getArrowTable().has_value()) {
	const auto & arrowArrays1 = tupleSet1->getArrowTable().value()->columns();
	const auto & arrowArrays2 = tupleSet2->getArrowTable().value()->columns();
	auto mergedArrays = std::vector<std::shared_ptr<::arrow::ChunkedArray>>{};
	mergedArrays.reserve(arrowArrays1.size() + arrowArrays2.size());

	mergedArrays.insert(std::end(mergedArrays), std::begin(arrowArrays1), std::end(arrowArrays1));
	mergedArrays.insert(std::end(mergedArrays), std::begin(arrowArrays2), std::end(arrowArrays2));

	return mergedArrays;
  } else {
	// Handle empty tupleset
	const auto & mergedArrays = std::vector<std::shared_ptr<::arrow::ChunkedArray>>{};
	return mergedArrays;
  }
}

tl::expected<std::shared_ptr<TupleSet2>, std::string>
MergeKernel::merge(const std::shared_ptr<TupleSet2> &tupleSet1,
				   const std::shared_ptr<TupleSet2> &tupleSet2) {

  // Validate the tuple sets
  const auto &expectedError = validateTupleSets(tupleSet1, tupleSet2);
  if (expectedError.has_value())
	return tl::unexpected(expectedError.value());

  // Merge schema
  const auto &mergedSchema = mergeSchema(tupleSet1, tupleSet2);

  // Merge arrays
  const auto &mergedArrays = mergeArrays(tupleSet1, tupleSet2);

  // Create the merged tuple set

  /**
   * FIXME: System is interchangeably using 0-length vectors and null optionals to represent
   *  an empty tupleset. Need to standardise on one way or the other. Preferring null optional
   *  for the time being as its less likely to hide empty tables not being handled properly.
   */
  if (!mergedSchema->fields().empty() && !mergedArrays.empty()) {
	const auto &mergedTable = ::arrow::Table::Make(mergedSchema->getSchema(), mergedArrays);
	const auto &mergedTupleSet = TupleSet2::make(mergedTable);
	return mergedTupleSet;
  } else {
	const auto &mergedTupleSet = TupleSet2::make();
	return mergedTupleSet;
  }

}





