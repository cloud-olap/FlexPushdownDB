//
// Created by matt on 20/7/20.
//

#include <fpdb/executor/physical/merge/MergeKernel.h>
#include <tl/expected.hpp>

using namespace fpdb::executor::physical::merge;

tl::expected<void, std::string> MergeKernel::validateTupleSets(const std::shared_ptr<TupleSet> &tupleSet1,
														  const std::shared_ptr<TupleSet> &tupleSet2) {

  // If either of the tuple sets contain columns (meaning they need to be merged) then check
  // they are merge-able. If either tuple set contains no columns, then merging them simply
  // means discarding the empty one.
  if (tupleSet1->numColumns() > 0 && tupleSet2->numColumns() > 0) {

    // Check row count is equal
    if (tupleSet1->numRows() != tupleSet2->numRows()) {
      return tl::unexpected(fmt::format(
              "Cannot merge TupleSets, number of rows must be equal. "
              "tupleSet1.numRows: {} != tupleSet2.numRows: {}",
              tupleSet1->numRows(), tupleSet2->numRows()));
    }
  }

  return {};
}

std::shared_ptr<Schema> MergeKernel::mergeSchema(const std::shared_ptr<TupleSet> &tupleSet1,
												 const std::shared_ptr<TupleSet> &tupleSet2) {

  std::set<std::string> fields1NameSet;
  auto mergedFields = std::vector<std::shared_ptr<arrow::Field>>{};

  if (tupleSet1->valid()){
    auto fields1Names = tupleSet1->schema()->field_names();
    fields1NameSet = std::set<std::string>(fields1Names.begin(), fields1Names.end());
    const auto &fields1 = tupleSet1->schema()->fields();
    mergedFields.insert(std::end(mergedFields), std::begin(fields1), std::end(fields1));
  }

  if (tupleSet2->valid()) {
    const auto &fields2 = tupleSet2->schema()->fields();
    for (const auto &field: fields2) {
      // deduplicate
      if (fields1NameSet.find(field->name()) == fields1NameSet.end()) {
        mergedFields.emplace_back(field);
      }
    }
  }

  const auto &mergedArrowSchema = std::make_shared<::arrow::Schema>(mergedFields);
  const auto &mergedSchema = Schema::make(mergedArrowSchema);

  return mergedSchema;
}

std::vector<std::shared_ptr<::arrow::ChunkedArray>> MergeKernel::mergeArrays(const std::shared_ptr<TupleSet> &tupleSet1,
																			 const std::shared_ptr<TupleSet> &tupleSet2) {

  std::set<std::string> fields1NameSet;
  auto mergedArrays = std::vector<std::shared_ptr<::arrow::ChunkedArray>>{};

  if (tupleSet1->valid()) {
    auto fields1Names = tupleSet1->schema()->field_names();
    fields1NameSet = std::set<std::string>(fields1Names.begin(), fields1Names.end());
    const auto &arrowArrays1 = tupleSet1->table()->columns();
    mergedArrays.insert(std::end(mergedArrays), std::begin(arrowArrays1), std::end(arrowArrays1));
  }

  if (tupleSet2->valid()) {
    const auto &fields2 = tupleSet2->schema()->fields();
    const auto &arrowArrays2 = tupleSet2->table()->columns();
    for (int i = 0; i < tupleSet2->numColumns(); ++i) {
      // deduplicate
      if (fields1NameSet.find(fields2[i]->name()) == fields1NameSet.end()) {
        mergedArrays.emplace_back(arrowArrays2[i]);
      }
    }
  }

  return mergedArrays;
}

tl::expected<std::shared_ptr<TupleSet>, std::string>
MergeKernel::merge(const std::shared_ptr<TupleSet> &tupleSet1,
				   const std::shared_ptr<TupleSet> &tupleSet2) {

  // Validate the tuple sets
  const auto valid = validateTupleSets(tupleSet1, tupleSet2);
  if (!valid)
	return tl::unexpected(valid.error());

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
	const auto &mergedTupleSet = TupleSet::make(mergedTable);
	return mergedTupleSet;
  } else {
	const auto &mergedTupleSet = TupleSet::makeWithEmptyTable();
	return mergedTupleSet;
  }

}





