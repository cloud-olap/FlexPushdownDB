//
// Created by matt on 8/10/20.
//

#include <doctest/doctest.h>
#include <nanobench.h>

#include <normal/tuple/arrow/Arrays.h>
#include <normal/tuple/ArrayAppenderWrapper.h>
#include <normal/tuple/Sample.h>

#include "Globals.h"

using namespace normal::tuple;

TEST_SUITE ("array-appender-benchmark" * doctest::skip(false)) {

TEST_CASE ("array-appender-benchmark-append-unsafe" * doctest::skip(false)) {

  auto sourceArray = Sample::sampleCxRString(1, 1000)->getArrowTable().value()->column(0)->chunk(0);

  ankerl::nanobench::Config().minEpochIterations(1).run(
	  getCurrentTestName(), [&] {
		auto appender = ArrayAppenderBuilder::make(::arrow::utf8(), 0).value();
		for (int i = 0; i < sourceArray->length(); ++i) {
		  auto result = appender->safeAppendValue(sourceArray, i);
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
