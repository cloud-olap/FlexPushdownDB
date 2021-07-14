//
// Created by Yifei Yang on 11/3/20.
//

#include <memory>

#include <doctest/doctest.h>

#include <nanobench.h>

#include <normal/tuple/Sample.h>
#include <normal/expression/gandiva/Column.h>
#include <normal/core/type/Integer32Type.h>
#include <normal/expression/gandiva/Cast.h>
#include <normal/expression/gandiva/And.h>
#include <normal/expression/gandiva/LessThan.h>
#include <normal/expression/gandiva/LessThanOrEqualTo.h>
#include <normal/expression/gandiva/GreaterThan.h>
#include <normal/expression/gandiva/GreaterThanOrEqualTo.h>
#include <normal/expression/gandiva/NumericLiteral.h>
#include <normal/tuple/TupleSet2.h>
#include <normal/expression/gandiva/Filter.h>

#define SKIP_SUITE true

using namespace normal::tuple;
using namespace normal::core::type;
using namespace normal::expression::gandiva;

namespace {
void run1(const std::shared_ptr<TupleSet2> &stringTupleSet, const std::shared_ptr<TupleSet2> &intTupleSet) {

//  SPDLOG_INFO("Input:\n{}", intTupleSet->showString(TupleSetShowOptions(TupleSetShowOrientation::RowOriented)));
  std::shared_ptr<TupleSet2> outputTupleSet;

  ankerl::nanobench::Config().minEpochIterations(5).run(
    fmt::format("filter-{}-rows of string", stringTupleSet->numRows()), [&] {
        auto comp1 = gt(cast(col("c_0"), integer32Type()), num_lit<::arrow::Int32Type, int>(4));
        auto comp2 = lt(cast(col("c_0"), integer32Type()), num_lit<::arrow::Int32Type, int>(6));
        auto comp3 = gte(cast(col("c_1"), integer32Type()), num_lit<::arrow::Int32Type, int>(0));
        auto comp4 = lte(cast(col("c_1"), integer32Type()), num_lit<::arrow::Int32Type, int>(10));
        auto expr = and_(and_(and_(comp1, comp2), comp3), comp4);

        auto filter = normal::expression::gandiva::Filter::make(expr);
        filter->compile(stringTupleSet->schema().value());

        outputTupleSet = filter->evaluate(*stringTupleSet);
    });

//  SPDLOG_INFO("Output:\n{}", outputTupleSet->showString(TupleSetShowOptions(TupleSetShowOrientation::RowOriented)));

  ankerl::nanobench::Config().minEpochIterations(5).run(
    fmt::format("filter-{}-rows of int", intTupleSet->numRows()), [&] {
        auto comp1 = gt(col("c_0"), num_lit<::arrow::Int32Type, int>(4));
        auto comp2 = lt(col("c_0"), num_lit<::arrow::Int32Type, int>(6));
        auto comp3 = gte(col("c_1"), num_lit<::arrow::Int32Type, int>(0));
        auto comp4 = lte(col("c_1"), num_lit<::arrow::Int32Type, int>(10));
        auto expr = and_(and_(and_(comp1, comp2), comp3), comp4);

        auto filter = normal::expression::gandiva::Filter::make(expr);
        filter->compile(intTupleSet->schema().value());

        outputTupleSet = filter->evaluate(*intTupleSet);
    });
}

void run2(const std::shared_ptr<TupleSet2> &inputTupleSet) {

//  SPDLOG_INFO("Input:\n{}", intTupleSet->showString(TupleSetShowOptions(TupleSetShowOrientation::RowOriented)));
  std::shared_ptr<TupleSet2> outputTupleSet;

  ankerl::nanobench::Config().minEpochIterations(5).run(
    fmt::format("filter-{}-rows of string - 4 preds", inputTupleSet->numRows()), [&] {
        auto comp1 = gt(cast(col("c_0"), integer32Type()), num_lit<::arrow::Int32Type, int>(4));
        auto comp2 = lt(cast(col("c_0"), integer32Type()), num_lit<::arrow::Int32Type, int>(6));
        auto comp3 = gte(cast(col("c_1"), integer32Type()), num_lit<::arrow::Int32Type, int>(0));
        auto comp4 = lte(cast(col("c_1"), integer32Type()), num_lit<::arrow::Int32Type, int>(10));
        auto expr = and_(and_(and_(comp1, comp2), comp3), comp4);

        auto filter = normal::expression::gandiva::Filter::make(expr);
        filter->compile(inputTupleSet->schema().value());

        outputTupleSet = filter->evaluate(*inputTupleSet);
    });

//  SPDLOG_INFO("Output:\n{}", outputTupleSet->showString(TupleSetShowOptions(TupleSetShowOrientation::RowOriented)));

  ankerl::nanobench::Config().minEpochIterations(5).run(
    fmt::format("filter-{}-rows of string - 2 preds", inputTupleSet->numRows()), [&] {
        auto comp1 = gt(cast(col("c_0"), integer32Type()), num_lit<::arrow::Int32Type, int>(4));
        auto comp2 = lt(cast(col("c_0"), integer32Type()), num_lit<::arrow::Int32Type, int>(6));
        auto expr = and_(comp1, comp2);

        auto filter = normal::expression::gandiva::Filter::make(expr);
        filter->compile(inputTupleSet->schema().value());

        outputTupleSet = filter->evaluate(*inputTupleSet);
    });
}
}

TEST_SUITE ("filter-benchmark" * doctest::skip(SKIP_SUITE)) {

TEST_CASE ("filter-benchmark" * doctest::skip(false || SKIP_SUITE)) {

  auto tupleSet10String = Sample::sampleCxRIntString(10, 10);
  auto tupleSet100String = Sample::sampleCxRIntString(10, 100);
  auto tupleSet1000String = Sample::sampleCxRIntString(10, 1000);
  auto tupleSet10000String = Sample::sampleCxRIntString(10, 10000);
  auto tupleSet100000String = Sample::sampleCxRIntString(10, 100000);

  auto tupleSet10Int = Sample::sampleCxRInt<int, ::arrow::Int32Type>(10, 10);
  auto tupleSet100Int = Sample::sampleCxRInt<int, ::arrow::Int32Type>(10, 100);
  auto tupleSet1000Int = Sample::sampleCxRInt<int, ::arrow::Int32Type>(10, 1000);
  auto tupleSet10000Int = Sample::sampleCxRInt<int, ::arrow::Int32Type>(10, 10000);
  auto tupleSet100000Int = Sample::sampleCxRInt<int, ::arrow::Int32Type>(10, 100000);

//  run1(tupleSet10String, tupleSet10Int);
//  run1(tupleSet100String, tupleSet100Int);
//  run1(tupleSet1000String, tupleSet1000Int);
//  run1(tupleSet10000String, tupleSet10000Int);
  run1(tupleSet100000String, tupleSet100000Int);

//  run2(tupleSet10String);
//  run2(tupleSet100String);
//  run2(tupleSet1000String);
//  run2(tupleSet10000String);
  run2(tupleSet100000String);
}

}