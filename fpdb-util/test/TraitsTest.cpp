//
// Created by matt on 6/10/20.
//

#include <type_traits>

#include <doctest/doctest.h>

#include <fpdb/util/Traits.h>

#include "Globals.h"

using namespace fpdb::util;

#define SKIP_SUITE true

TEST_SUITE ("traits" * doctest::skip(SKIP_SUITE)) {

TEST_CASE ("traits-lambda" * doctest::skip(false || SKIP_SUITE)) {

  {
	auto test = [](int a) mutable -> long { return static_cast<long>(a); };

	using Lambda = decltype(test);

	using ReturnType = boost::callable_traits::return_type_t<Lambda>;
	SPDLOG_DEBUG("Return type 1: {}", typeid(ReturnType).name());
		CHECK(std::is_same<boost::callable_traits::return_type_t<decltype(test)>, long>::value);

	using Args = boost::callable_traits::args_t<Lambda>;

	SPDLOG_DEBUG("Arity: {}", std::tuple_size_v<Args>);

	SPDLOG_DEBUG("Mutable: {}", !boost::callable_traits::is_const_member_v<Lambda>);

	using Arg1 = std::tuple_element_t<0, Args>;
	SPDLOG_DEBUG("Arg type 1: {}", typeid(Arg1).name());
		CHECK(std::is_same<Arg1, int>::value);
  }

  {
	auto test = [](int a) -> long { return static_cast<long>(a); };

	using Lambda = decltype(test);

	using ReturnType = boost::callable_traits::return_type_t<Lambda>;
	SPDLOG_DEBUG("Return type 1: {}", typeid(ReturnType).name());
		CHECK(std::is_same<boost::callable_traits::return_type_t<decltype(test)>, long>::value);

	using Args = boost::callable_traits::args_t<Lambda>;

	SPDLOG_DEBUG("Arity: {}", std::tuple_size_v<Args>);

	SPDLOG_DEBUG("Mutable: {}", !boost::callable_traits::is_const_member_v<Lambda>);

	using Arg1 = std::tuple_element_t<0, Args>;
	SPDLOG_DEBUG("Arg type 1: {}", typeid(Arg1).name());
		CHECK(std::is_same<Arg1, int>::value);
  }

  {
	auto test = [](int a, float /* b */) -> long { return static_cast<long>(a); };

	using Lambda = decltype(test);

	using ReturnType = boost::callable_traits::return_type_t<Lambda>;
	SPDLOG_DEBUG("Return type 1: {}", typeid(ReturnType).name());
		CHECK(std::is_same<boost::callable_traits::return_type_t<decltype(test)>, long>::value);

	using Args = boost::callable_traits::args_t<Lambda>;

	SPDLOG_DEBUG("Arity: {}", std::tuple_size_v<Args>);

	SPDLOG_DEBUG("Mutable: {}", !boost::callable_traits::is_const_member_v<Lambda>);

	using Arg1 = std::tuple_element_t<0, Args>;
	SPDLOG_DEBUG("Arg type 1: {}", typeid(Arg1).name());
		CHECK(std::is_same<Arg1, int>::value);

	using Arg2 = std::tuple_element_t<1, Args>;
	SPDLOG_DEBUG("Arg type 2: {}", typeid(Arg2).name());
		CHECK(std::is_same<Arg2, float>::value);
  }

}

}