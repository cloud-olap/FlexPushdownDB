"""
NOTE: To calculate the best possible fp rate for a bloom filter we need to calculate the best achievable fp rate for a
given capacity and bit array size.

We know the capacity but need to calculate the bit array size. We know the max size of an S3 select expression, but we
also need to take into account the number of characters used by the SQL statement that uses the bloom filter predicate
and the extra SQL needed for the predicate itself.

However, it's difficult to know precisely what the hash function SQL will be (without actually building it). It can
approximated though, since we know the bloom filter predicates will be of the form (BF SQL template):

    substring('<SLICE_BIT_ARRAY_STRING>', <HASH_FUNCTION_STR> + 1, 1) = '1'
    substring('',  + 1, 1) = '1' (28 chars)

where HASH_FUNCTION_STR is:

    ( ( <INT> * cast(<FIELD_NAME> as int) + <INT> ) % <INT> ) % <INT>
    ( (  * cast( as int) +  ) %  ) % (32 chars)

where 8 chars are reserved for each <INT> and 16 chars are reserved for <FIELD_NAME>

Given this, we approximate the bloom filter predicate lengths as:

    m + (k * (28 + 32 + 8 + 16))
    m + (k * 84)

where m is the size of the bit array and k is the number of hash functions/slices. Since we don't know the value of k
until an fp rate is calculated, we assume a maximum for k of 13 - which is a very restrictive bloom filter of fp rate
0.0001.

Solving for m:

m = MAX_S3_SELECT_EXPRESSION_LEN - ENCLOSING_SQL_LEN - (13 * 84)
m = MAX_S3_SELECT_EXPRESSION_LEN - ENCLOSING_SQL_LEN - 1092

"""

import numpy

from s3filter.hash.sliced_bloom_filter import SlicedBloomFilter

MAX_S3_SELECT_EXPRESSION_LEN = 1024 * 256
MAX_BLOOM_FILTER_PREDICATE_SQL_TEMPLATE_LEN = 1092


class SlicedSQLBloomFilter(object):
    """Wrapper class for a sliced bloom filter that adds the methods to convert it into it's SQL representation so it
    can be pushed down to s3

    """

    def __init__(self, sliced_bloom_filter=None):
        if sliced_bloom_filter is not None:
            self._bloom_filter = sliced_bloom_filter

    def fp_rate(self):
        return self._bloom_filter.error_rate

    @staticmethod
    def build(sliced_bloom_filter):
        return SlicedSQLBloomFilter(sliced_bloom_filter)

    def add(self, k):
        self._bloom_filter.add(k)

    def build_bit_array_string_sql_predicate(self, field):
        """SQL expression representing a predicate that can be used to push down the bloom filter into an SQL
        statement. It converts the hash functions to their SQL expression equivalent and uses that as an index
        into a s3 select substring function that tests for the presence of a '1' in the bloom filters bit array
        which is converted to a string of 0's and 1's.

        :param field: The name of the field (column) containing the key data we want to test against
        :return: SQL string
        """

        sql = ""

        for i, slice_bit_array in enumerate(self._bloom_filter.bit_arrays):

            # NOTE: S3 Select SUBSTRING fails if given an empty string, so simply use False if the bit array is empty
            if len(slice_bit_array) == 0:
                sql += " False "
            else:
                hash_fn = self._bloom_filter.hash_functions[i]
                hash_fn_sql = hash_fn.sql(field)
                bit_array_str = "".join(map(lambda b: "1" if b else "0", slice_bit_array))
                sql += " substring('" + bit_array_str + "', " + hash_fn_sql + " + 1, 1) = '1' "

            if i < len(self._bloom_filter.bit_arrays) - 1:
                sql += " and "

        return sql

    def build_hash_functions_sql_projection(self, field):
        """Creates a SQL projection of the hash functions in the bloom filter. Useful for debugging.

        :param field: The field to calculate the hash of
        :return: SQL projection
        """

        sql = ""

        for i, hash_fn in enumerate(self._bloom_filter.hash_functions):
            hash_fn_sql = hash_fn.sql(field)
            sql += hash_fn_sql
            if i < len(self._bloom_filter.hash_functions) - 1:
                sql += ", "

        return sql

    def build_bit_array_strings_sql_projection(self):
        """Creates a SQL projection of the bit arrays in the bloom filter as strings of 1's and 0's. Useful for
        debugging.

        :return: SQL projection
        """

        sql = ""

        for i, slice_bit_array in enumerate(self._bloom_filter.bit_arrays):
            bit_array_str = "".join(map(lambda x: "1" if x else "0", slice_bit_array))
            sql += "'" + bit_array_str + "'"
            if i < len(self._bloom_filter.bit_arrays) - 1:
                sql += ", "

        return sql

    def build_bit_array_index_list_sql_predicate(self, field):
        """SQL expression representing a predicate that can be used to push down the bloom filter into an SQL statement.

        :param field: The name of the field (column) containing the key data we want to tst against
        :return: SQL string
        """

        sql = ""

        bit_indexes_list = self.bit_arrays_as_index_arrays()

        for slice_index in range(0, len(bit_indexes_list)):
            hash_fn = self._bloom_filter.hash_functions[slice_index]
            hash_fn_sql = hash_fn.sql(field)
            bit_index_array_str = ",".join(map(str, bit_indexes_list[slice_index]))
            sql += hash_fn_sql + " in (" + bit_index_array_str + ")"
            if slice_index < len(bit_indexes_list) - 1:
                sql += " and "

        return sql

    def bit_arrays_as_index_arrays(self):
        """Converts the bit arrays into an array of indexes, one array of indexes per slice

        :return: Array of indexes
        """

        index_arrays = []
        for slice_bit_array in self._bloom_filter.bit_arrays:
            slice_index_array = numpy.where(slice_bit_array)[0]
            index_arrays.append(slice_index_array)

        return index_arrays

    @staticmethod
    def calc_best_fp_rate(capacity, enclosing_sql_len):
        """
        Given a capacity, calculates the best achievable false positive rate

        :param enclosing_sql_len:
        :param capacity:
        :return:
        """

        # Figure out how many bytes we have available for the SQL
        m = MAX_S3_SELECT_EXPRESSION_LEN - (enclosing_sql_len + MAX_BLOOM_FILTER_PREDICATE_SQL_TEMPLATE_LEN)
        kp = SlicedBloomFilter.kp_from_mn(m, capacity)

        return kp[1]
