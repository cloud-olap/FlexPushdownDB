//
// Created by matt on 8/5/20.
//

#include <doctest/doctest.h>

#include <gandiva/function_registry.h>

#include "Globals.h"

TEST_SUITE ("gandiva-function-registry" * doctest::skip(true)) {

TEST_CASE ("show-functions") {
  ::gandiva::FunctionRegistry registry;
  for (auto native_func_it = registry.begin(); native_func_it != registry.end(); ++native_func_it) {
	SPDLOG_DEBUG("Function  |  pc_name: {}", native_func_it->pc_name());

	for (auto &sig : native_func_it->signatures()) {
	  auto sig_str = sig.ToString();
	  SPDLOG_DEBUG("          |  signature: {}", sig_str);
	}
  }
}

}