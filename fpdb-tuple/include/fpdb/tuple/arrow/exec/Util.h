//
// Created by Yifei Yang on 11/23/22.
//

#ifndef FPDB_FPDB_TUPLE_INCLUDE_FPDB_TUPLE_ARROW_EXEC_UTIL_H
#define FPDB_FPDB_TUPLE_INCLUDE_FPDB_TUPLE_ARROW_EXEC_UTIL_H

#if defined(__clang__) || defined(__GNUC__)
#define ROTL64(x, n) (((x) << (n)) | ((x) >> ((-n) & 63)))
#define PREFETCH(ptr) __builtin_prefetch((ptr), 0 /* rw==read */, 3 /* locality */)
#elif defined(_MSC_VER)
#include <intrin.h>
#define ROTL64(x, n) _rotl64((x), (n))
#if defined(_M_X64) || defined(_M_I86)
#include <mmintrin.h>  // https://msdn.microsoft.com/fr-fr/library/84szxsww(v=vs.90).aspx
#define PREFETCH(ptr) _mm_prefetch((const char*)(ptr), _MM_HINT_T0)
#else
#define PREFETCH(ptr) (void)(ptr) /* disabled */
#endif
#endif

#endif //FPDB_FPDB_TUPLE_INCLUDE_FPDB_TUPLE_ARROW_EXEC_UTIL_H
