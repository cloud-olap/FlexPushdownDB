# -*- encoding: utf-8 -*-
"""Bloom filter support

"""

import math
import numpy
from s3filter.hash.universal_sql_hash import UniversalSQLHashFunction


class BloomFilter(object):

    def __init__(self, capacity, error_rate):

        if not capacity > 0:
            raise Exception("Capacity must be > 0")
        if not (0 < error_rate < 1):
            raise Exception("Illegal error rate {}. Error rate must be between 0 and 1.".format(error_rate))

        n = capacity
        p = error_rate

        # self.num_bits = ((0 - capacity) * math.log(error_rate) / (math.log(2) * math.log(2)))
        m = int(math.ceil((n * math.log(p)) / math.log(1 / pow(2, math.log(2)))))
        k = max(1, int(round((m / n) * math.log(2))))

        self.capacity = n
        self.num_slices = k
        self.num_bits = m
        self.error_rate = p

        self.hash_functions = self.build_hash_functions()

        self.bit_arrays = numpy.zeros(self.num_bits, dtype=bool)

        self.count = 0

    def build_hash_functions(self):
        """Creates the hash functions needed for the bloom filter. One per slice with each function hashing over the number
        of bits in each slice.

        :return: List of hash functions
        """

        hash_fns = []

        for _ in range(0, self.num_slices):
            hash_fn = UniversalSQLHashFunction(self.num_bits)
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
            if not self.bit_arrays[h]:
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
            if found_all_bits and not self.bit_arrays[h]:
                found_all_bits = False
            self.bit_arrays[h] = True
            slice_index += 1

        if not found_all_bits:
            self.count += 1
            return False
        else:
            return True

    def sql_predicate(self, field):
        """SQL expression representing a predicate that can be used to push down the bloom filter into an SQL statement.

        :param field: The name of the field (column) containing the key data we want to tst against
        :return: SQL string
        """

        sql = ""

        bit_indexes_list = self.bit_arrays_as_index_arrays()

        for slice_index in range(0, len(bit_indexes_list)):
            hash_fn = self.hash_functions[slice_index]
            slice_field_str = hash_fn.sql(field)
            slice_set_str = ",".join(map(str, bit_indexes_list[slice_index]))
            sql += slice_field_str + " in (" + slice_set_str + ")"
            if slice_index < len(bit_indexes_list) - 1:
                sql += " and "

        return sql

    def bit_arrays_as_index_arrays(self):
        """Converts the bit arrays into an array of indexes, one array of indexes per slice

        :return: Array of indexes
        """

        index_arrays = []
        for slice_bit_array in self.bit_arrays:
            slice_index_array = numpy.where(slice_bit_array)[0]
            index_arrays.append(slice_index_array)

        return index_arrays
