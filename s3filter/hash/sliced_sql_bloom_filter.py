import numpy


class SlicedSQLBloomFilter(object):
    """Wrapper class for a sliced bloom filter that adds the methods to convert it into it's SQL representation so it
    can be pushed down to s3

    """

    def __init__(self, sliced_bloom_filter):
        self._bloom_filter = sliced_bloom_filter

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
