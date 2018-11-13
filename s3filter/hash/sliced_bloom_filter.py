# -*- encoding: utf-8 -*-
"""Bloom filter support

"""

import math
import numpy
from s3filter.hash.universal_sql_hash import UniversalSQLHashFunction


class SlicedBloomFilter(object):
    """Bloom filter implementation based off the scalable bloom filter described by Baquero, Preguica, Hutchison.

    Intended to be created as needed by ScalableBloomFilter as new capcity is required.

    Uses universal hash functions for integers to support dynamic creation of N hash functions for each slice in the
    filter.

    Inspired by pybloom library

    """

    def __init__(self, capacity, error_rate):
        """Creates a new bloom filter that will attempt to satisfy the given parameters. The filter will create
        internal bit lists and slices/hash functions to support the given target capacity and false positive rates.

        :param capacity: How many elements the filter needs to manage
        :param error_rate: The desired false positive rate that needs to be supported
        """

        # if not capacity > 0:
        #     raise Exception("Capacity must be > 0")
        # if not (0 < error_rate < 1):
        #     raise Exception("Illegal error rate {}. Error rate must be between 0 and 1.".format(error_rate))

        self.capacity = capacity
        self.error_rate = error_rate

        self.num_slices = SlicedBloomFilter.k_from_p(error_rate)
        self.num_bits_per_slice = SlicedBloomFilter.o_from_npk(self.capacity, self.error_rate, self.num_slices)

        self.num_bits = self.m_from_ko(self.num_slices, self.num_bits_per_slice)
        self.count = 0

        self.hash_functions = self.build_hash_functions()

        self.bit_arrays = numpy.zeros((self.num_slices, self.num_bits_per_slice), dtype=bool)

    def __repr__(self):
        return {
            'capacity': self.capacity,
            'error_rate': self.error_rate,
            'num_slices': self.num_slices,
            'num_bits_per_slice': self.num_bits_per_slice,
            'num_bits': self.num_slices * self.num_bits_per_slice
        }.__repr__()

    def build_hash_functions(self):
        """Creates the hash functions needed for the bloom filter. One per slice with each function hashing over the number
        of bits in each slice.

        :return: List of hash functions
        """

        hash_fns = []

        for _ in range(0, self.num_slices):
            hash_fn = UniversalSQLHashFunction(self.num_bits_per_slice)
            hash_fns.append(hash_fn)

        return hash_fns

    def __contains__(self, key):
        """Tests whether the filter contains the given key. As per bloom filter semantics may return false positives.

        :param key: The key to tst
        :return: True of false
        """

        hashes = [fn(key) for fn in self.hash_functions]
        slice_index = 0
        for h in hashes:
            if not self.bit_arrays[slice_index][h]:
                return False
            slice_index += 1
        return True

    def __len__(self):
        """
        :return: Size of the bloom filter
        """

        return self.count

    def add(self, key):
        """Adds the given key to the bloom filter

        :param key:
        :return:
        """

        hashes = [fn(key) for fn in self.hash_functions]
        found_all_bits = True
        if self.count > self.capacity:
            raise RuntimeError("BloomFilter overflow. Element count {} exceeds capacity {}"
                               .format(self.count, self.capacity))
        slice_index = 0
        for h in hashes:
            if found_all_bits and not self.bit_arrays[slice_index][h]:
                found_all_bits = False
            self.bit_arrays[slice_index][h] = True
            slice_index += 1

        if not found_all_bits:
            self.count += 1
            return False
        else:
            return True

    @staticmethod
    def o_from_npk(n, p, k):
        """
        given M = num_bits, k = num_slices, P = error_rate, n = capacity
              k = log2(1/P)
        solving for o = bits_per_slice
        n ~= M * ((ln(2) ** 2) / abs(ln(P)))
        n ~= (k * m) * ((ln(2) ** 2) / abs(ln(P)))
        m ~= n * abs(ln(P)) / (k * (ln(2) ** 2))

        :param n:
        :param p:
        :param k:
        :return:
        """

        if k == 0:
            return 0
        else:
            return int(math.ceil((n * abs(math.log(p))) / (k * (math.log(2) ** 2))))

    @staticmethod
    def k_from_p(p):
        return int(math.ceil(math.log(1.0 / float(p), 2)))

    @staticmethod
    def r_from_mn(m, n):
        return float(m) / float(n)

    @staticmethod
    def p_from_kr(k, r):
        return math.pow(1 - math.exp((0 - k) / r), k)

    @staticmethod
    def p_from_kmn(k, m, n):
        r = SlicedBloomFilter.r_from_mn(m, n)
        return SlicedBloomFilter.p_from_kr(k, r)

    @staticmethod
    def k_from_r(r):
        return round(math.log(2) * r)

    @staticmethod
    def m_from_ko(k, o):
        return k * o

    @staticmethod
    def kp_from_mn(m, n):
        r = SlicedBloomFilter.r_from_mn(m, n)
        k = SlicedBloomFilter.k_from_r(r)
        p = SlicedBloomFilter.p_from_kr(k, r)

        return k, p
