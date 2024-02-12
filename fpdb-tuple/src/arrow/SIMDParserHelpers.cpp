//
// Created by Matt Woicik on 3/1/21.
//

// The code in this file is taken from:
// https://github.com/geofflangdale/simdcsv

#ifdef __AVX2__
#include "fpdb/tuple/arrow/SIMDParserHelpers.h"

#include <bitset>

inline simd_input fill_input(const uint8_t * ptr) {
  simd_input in;
  in.lo = _mm256_loadu_si256(reinterpret_cast<const __m256i *>(ptr + 0));
  in.hi = _mm256_loadu_si256(reinterpret_cast<const __m256i *>(ptr + 32));
//  std::stringstream ss;
//  ss << "in.lo: ";
//  for (int i = 0; i < 32; i++){
//    ss << ptr[i];
//  }
//  ss << "\n";
//  ss << "in.hi: ";
//  for (int i = 32; i < 64; i++){
//    ss << ptr[i];
//  }
//  ss << "\n";
//  SPDLOG_DEBUG("\n{}", ss.str());
  return in;
}

/* result might be undefined when input_num is zero */
static inline int trailingzeroes(uint64_t input_num) {
#ifdef __BMI2__
	return _tzcnt_u64(input_num);
#else
	return __builtin_ctzll(input_num);
#endif
}

/* result might be undefined when input_num is zero */
static inline int hamming(uint64_t input_num) {
#ifdef __POPCOUNT__
	return _popcnt64(input_num);
#else
	return __builtin_popcountll(input_num);
#endif
}

// a straightforward comparison of a mask against input. 5 uops; would be
// cheaper in AVX512.
inline uint64_t cmp_mask_against_input(simd_input in, uint8_t m) {
  const __m256i mask = _mm256_set1_epi8(m);
  __m256i cmp_res_0 = _mm256_cmpeq_epi8(in.lo, mask);
  uint64_t res_0 = static_cast<uint32_t>(_mm256_movemask_epi8(cmp_res_0));
  __m256i cmp_res_1 = _mm256_cmpeq_epi8(in.hi, mask);
  uint64_t res_1 = _mm256_movemask_epi8(cmp_res_1);
  return res_0 | (res_1 << 32);
}


// return the quote mask (which is a half-open mask that covers the first
// quote in a quote pair and everything in the quote pair)
// We also update the prev_iter_inside_quote value to
// tell the next iteration whether we finished the final iteration inside a
// quote pair; if so, this  inverts our behavior of  whether we're inside
// quotes for the next iteration.

inline uint64_t find_quote_mask(simd_input in, uint64_t &prev_iter_inside_quote) {
  uint64_t quote_bits = cmp_mask_against_input(in, '"');
//  std::bitset<64> x(quote_bits);
//  std::stringstream quote_bits_ss;
//  quote_bits_ss << x;
//  SPDLOG_DEBUG("quote bits: {}", quote_bits_ss.str());

  uint64_t quote_mask = _mm_cvtsi128_si64(_mm_clmulepi64_si128(
      _mm_set_epi64x(0ULL, quote_bits), _mm_set1_epi8(0xFF), 0));
  quote_mask ^= prev_iter_inside_quote;

  // right shift of a signed value expected to be well-defined and standard
  // compliant as of C++20,
  // John Regher from Utah U. says this is fine code
  prev_iter_inside_quote =
      static_cast<uint64_t>(static_cast<int64_t>(quote_mask) >> 63);
  return quote_mask;
}


// flatten out values in 'bits' assuming that they are are to have values of idx
// plus their position in the bitvector, and store these indexes at
// base_ptr[base] incrementing base as we go
// will potentially store extra values beyond end of valid bits, so base_ptr
// needs to be large enough to handle this
inline void flatten_bits(uint32_t *base_ptr, uint32_t &base,
                                uint32_t idx, uint64_t bits) {
  if (bits != 0u) {
    uint32_t cnt = hamming(bits);
    uint32_t next_base = base + cnt;
    base_ptr[base + 0] = static_cast<uint32_t>(idx) + trailingzeroes(bits);
    bits = bits & (bits - 1);
    base_ptr[base + 1] = static_cast<uint32_t>(idx) + trailingzeroes(bits);
    bits = bits & (bits - 1);
    base_ptr[base + 2] = static_cast<uint32_t>(idx) + trailingzeroes(bits);
    bits = bits & (bits - 1);
    base_ptr[base + 3] = static_cast<uint32_t>(idx) + trailingzeroes(bits);
    bits = bits & (bits - 1);
    base_ptr[base + 4] = static_cast<uint32_t>(idx) + trailingzeroes(bits);
    bits = bits & (bits - 1);
    base_ptr[base + 5] = static_cast<uint32_t>(idx) + trailingzeroes(bits);
    bits = bits & (bits - 1);
    base_ptr[base + 6] = static_cast<uint32_t>(idx) + trailingzeroes(bits);
    bits = bits & (bits - 1);
    base_ptr[base + 7] = static_cast<uint32_t>(idx) + trailingzeroes(bits);
    bits = bits & (bits - 1);
    if (cnt > 8) {
      base_ptr[base + 8] = static_cast<uint32_t>(idx) + trailingzeroes(bits);
      bits = bits & (bits - 1);
      base_ptr[base + 9] = static_cast<uint32_t>(idx) + trailingzeroes(bits);
      bits = bits & (bits - 1);
      base_ptr[base + 10] = static_cast<uint32_t>(idx) + trailingzeroes(bits);
      bits = bits & (bits - 1);
      base_ptr[base + 11] = static_cast<uint32_t>(idx) + trailingzeroes(bits);
      bits = bits & (bits - 1);
      base_ptr[base + 12] = static_cast<uint32_t>(idx) + trailingzeroes(bits);
      bits = bits & (bits - 1);
      base_ptr[base + 13] = static_cast<uint32_t>(idx) + trailingzeroes(bits);
      bits = bits & (bits - 1);
      base_ptr[base + 14] = static_cast<uint32_t>(idx) + trailingzeroes(bits);
      bits = bits & (bits - 1);
      base_ptr[base + 15] = static_cast<uint32_t>(idx) + trailingzeroes(bits);
      bits = bits & (bits - 1);
    }
    if (cnt > 16) {
      base += 16;
      do {
        base_ptr[base] = static_cast<uint32_t>(idx) + trailingzeroes(bits);
        bits = bits & (bits - 1);
        base++;
      } while (bits != 0);
    }
    base = next_base;
  }
}

bool find_indexes(const uint8_t * buf, size_t len, struct ParsedCSV & pcsv, char delimiter) {
  // does the previous iteration end inside a double-quote pair?
  uint64_t prev_iter_inside_quote = 0ULL;  // either all zeros or all ones

  size_t lenminus64 = len < 64 ? 0 : len - 64;
  size_t idx = 0;
  uint32_t *base_ptr = pcsv.indexes;
  uint32_t base = 0;
  // we do the index decoding in bulk for better pipelining.
#define SIMDCSV_BUFFERSIZE 4 // it seems to be about the sweetspot.
  if(lenminus64 > 64 * SIMDCSV_BUFFERSIZE) {
    uint64_t fields[SIMDCSV_BUFFERSIZE];
    for (; idx < lenminus64 - 64 * SIMDCSV_BUFFERSIZE + 1; idx += 64 * SIMDCSV_BUFFERSIZE) {
      for(size_t b = 0; b < SIMDCSV_BUFFERSIZE; b++){
//        SPDLOG_DEBUG("\nOn iteration : {}", iteration);
//        iteration++;
        size_t internal_idx = 64 * b + idx;

        simd_input in = fill_input(buf+internal_idx);
        uint64_t quote_mask = find_quote_mask(in, prev_iter_inside_quote);
//        std::bitset<64> x(quote_mask);
//        std::stringstream quote_mask_ss;
//        quote_mask_ss << x;
//        SPDLOG_DEBUG("quote mask:{}", quote_mask_ss.str());
        uint64_t sep = cmp_mask_against_input(in, delimiter);
        uint64_t end = cmp_mask_against_input(in, 0x0a);
        fields[b] = (end | sep) & ~quote_mask;
//        std::bitset<64> y(fields[b]);
//        std::stringstream fields_ss;
//        fields_ss << y;
//        SPDLOG_DEBUG("final field:{}", fields_ss.str());
      }
      for(size_t b = 0; b < SIMDCSV_BUFFERSIZE; b++){
        size_t internal_idx = 64 * b + idx;
        flatten_bits(base_ptr, base, internal_idx, fields[b]);
      }
    }
  }
  // tail end will be unbuffered
  for (; idx < lenminus64; idx += 64) {
      simd_input in = fill_input(buf+idx);
      uint64_t quote_mask = find_quote_mask(in, prev_iter_inside_quote);
      uint64_t sep = cmp_mask_against_input(in, delimiter);
      uint64_t end = cmp_mask_against_input(in, 0x0a);
    // note - a bit of a high-wire act here with quotes
    // we can't put something inside the quotes with the CR
    // then outside the quotes with LF so it's OK to "and off"
    // the quoted bits here. Some other quote convention would
    // need to be thought about carefully
      uint64_t field_sep = (end | sep) & ~quote_mask;
      flatten_bits(base_ptr, base, idx, field_sep);
  }
#undef SIMDCSV_BUFFERSIZE
  pcsv.n_indexes = base;
  return true;
}

#endif