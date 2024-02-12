//
// Created by matt on 8/10/20.
//

#include <doctest/doctest.h>
#include <nanobench.h>

#include <fpdb/tuple/arrow/Arrays.h>
#include <fpdb/tuple/ArrayAppenderWrapper.h>
#include <fpdb/tuple/util/Sample.h>

#include "Globals.h"

using namespace fpdb::tuple;
using namespace fpdb::tuple::util;

TEST_SUITE ("array-appender-benchmark" * doctest::skip(false)) {

TEST_CASE ("array-appender-benchmark-append-unsafe" * doctest::skip(false)) {

  auto sourceArray = Sample::sampleCxRString(1, 1000)->table()->column(0)->chunk(0);

  ankerl::nanobench::Config().minEpochIterations(1).run(
	  getCurrentTestName(), [&] {
		auto appender = ArrayAppenderBuilder::make(::arrow::utf8(), 0).value();
		for (int i = 0; i < sourceArray->length(); ++i) {
		  auto result = appender->appendValue(sourceArray, i);
		  if (!result) {
			throw std::runtime_error(result.error());
		  }
		  auto destArray = appender->finalize();
		  if (!destArray) {
			throw std::runtime_error(destArray.error());
		  }
		}
	  });

  ankerl::nanobench::Config().minEpochIterations(1).run(
	  getCurrentTestName(), [&] {
		auto appender = ArrayAppenderBuilder::make(::arrow::utf8(), 0).value();
		for (int i = 0; i < sourceArray->length(); ++i) {
		  appender->appendValue(sourceArray, i);
		  auto destArray = appender->finalize();
		  if (!destArray) {
			throw std::runtime_error(destArray.error());
		  }
		}
	  });
}

}
