//
// Created by matt on 6/10/20.
//

/**
 * Place holder for utility type traits
 *
 * At the moment just pulls in boost callable traits (the lib that doesn't pull in all of boost)
 */

#ifndef FPDB_FPDB_UTIL_INCLUDE_FPDB_UTIL_TRAITS_H
#define FPDB_FPDB_UTIL_INCLUDE_FPDB_UTIL_TRAITS_H

#include <tuple>
#include <type_traits>

#include <boost/callable_traits.hpp>

using namespace boost::callable_traits;

namespace fpdb::util {
}

#endif //FPDB_FPDB_UTIL_INCLUDE_FPDB_UTIL_TRAITS_H
