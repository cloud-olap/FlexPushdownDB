//
// Created by matt on 8/10/20.
//

#include <doctest/doctest.h>

#include <normal/tuple/arrow/Arrays.h>
#include <normal/tuple/ArrayAppenderWrapper.h>
#include <iostream>

using namespace normal::tuple;

TEST_SUITE ("array-appender-test" * doctest::skip(false)) {

TEST_CASE ("array-appender-test-append" * doctest::skip(false)) {

  auto v = std::vector{"1", "2", "3"};
  auto sourceArray = Arrays::make<::arrow::StringType>(v).value();

  auto appender = ArrayAppenderBuilder::make(::arrow::utf8(), 3).value();

  for (int i = 0; i < sourceArray->length(); ++i) {
		CHECK(appender->safeAppendValue(sourceArray, i));
  }

  auto destArray = appender->finalize();

  // Arrays should be equal
	  CHECK(destArray);
	  CHECK(destArray.value()->Equals(sourceArray));

}

TEST_CASE ("array-appender-test-append-empty" * doctest::skip(false)) {

  auto v = std::vector<std::string>{};
  auto sourceArray = Arrays::make<::arrow::StringType>(v).value();

  auto appender = ArrayAppenderBuilder::make(::arrow::utf8(), 3).value();

  for (int i = 0; i < sourceArray->length(); ++i) {
		CHECK(appender->safeAppendValue(sourceArray, i));
  }

  auto destArray = appender->finalize();

  // Arrays should be equal and empty
	  CHECK(destArray);
	  CHECK(destArray.value()->Equals(sourceArray));
	  CHECK_EQ(destArray.value()->length(), 0);
}

TEST_CASE ("array-appender-test-append-bad-array" * doctest::skip(false)) {

  std::shared_ptr<arrow::Array> sourceArray = nullptr;

  auto appender = ArrayAppenderBuilder::make(::arrow::utf8(), 3).value();

  // Should return error
  auto result = appender->safeAppendValue(sourceArray, 0);
	  CHECK_FALSE_MESSAGE(result, "Safe append succeeded when should have failed");
}

TEST_CASE ("array-appender-test-append-bad-index" * doctest::skip(false)) {

  auto v = std::vector{"1", "2", "3"};
  auto sourceArray = Arrays::make<::arrow::StringType>(v).value();

  auto appender = ArrayAppenderBuilder::make(::arrow::utf8(), 3).value();

  // Should return error
  auto result = appender->safeAppendValue(sourceArray, sourceArray->length() + 1);
	  CHECK_FALSE_MESSAGE(result, "Safe append succeeded when should have failed");
}

}
