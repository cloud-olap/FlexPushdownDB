# -*- coding: utf-8 -*-
"""Bloom filter support

"""


class Bloom(object):
    """What we're trying to do here is incur less traffic while running a query between the local system and the
    remote store.

    Locally, we have a list of keys that we want to retrieve. To reduce inbound traffic we only want to load those
    records that match the list of keys we have, but we don't want to send all those keys to the remote store as
    this will add a lot of outbound traffic.

    To get around these two problems we create a bloom filter of the records we want to load and then convert that
    bloom filter into a SQL predicate that we can 'push down' to s3. We then use the bloom filter to load only
    the records we want, without needing to send the list of record keys to s3.

    To do this we need to do two things, first to create and represent a bloom filter in SQL, and then use that
    bloom filter as a predicate in a where clause.

    Normally construction of a bloom filter would mean creation of an empty bit array. Adding values to the bloom
    filter would work by hashing the value k times. The resulting hashes would indicate which bit to set in the bit
    array.

    To represent this in SQL, we represent the bloom filter bit array with a list, where the list contains the index
    of each bit set in the bit array.

    E.g. A 10-bit bit array [0,1,1,0,0,0,1,0,1,0] would be represented as (1,2,6,8).

    To test whether a value is present in the bloom filter we need to interrogate the list k times, once for each hash
    function.

    This can be satisfied with the following where clause. Given hash1, hash_2, and hash_n are our hash functions and
    bit_array_indexes is our list of bit array indexes that each has function has hashed to (in other words, the
    underlying bloom filter bit array)

    where
        hash_1(field) in bit_array_indexes and
        hash_2(field) in bit_array_indexes and
        hash_k(field) in bit_array_indexes

    The returned records should be those with the key we want to retrieve, in addition to some false positives. In
    example implemented below, 400 records should be returned, even though there are 200000 records in the table. At
    worst we will only ever send a maximum of 1000 keys to s3 (which is the maximum size of our bit indexes array.

    """

    def __init__(self):
        self.__bit_array_indexes = []

    # Our 3 hash functions (in Python and SQL)
    @staticmethod
    def __hash_3(v):
        return ((int(v) + 1) * int(v)) % 1000

    @staticmethod
    def __hash_3_sql(field):
        return "((cast({} as int) + 1) * cast({} as int)) % 1000".format(field, field)

    @staticmethod
    def __hash_2(v):
        return (int(v) * int(v)) % 1000

    @staticmethod
    def __hash_2_sql(field):
        return "(cast({} as int) * cast({} as int)) % 1000".format(field, field)

    @staticmethod
    def __hash_1(v):
        return int(v) % 1000

    @staticmethod
    def __hash_1_sql(field):
        return "cast({} as int) % 1000".format(field)

    def __ensure_set(self, v):
        if v not in self.__bit_array_indexes:
            self.__bit_array_indexes.append(v)

    def sql_predicate(self, field):

        bit_array_indexes_str = ",".join(map(str, self.__bit_array_indexes))

        return "{} in ({}) and " \
               "{} in ({}) and " \
               "{} in ({}) "\
            .format(self.__hash_1_sql(field),
                    bit_array_indexes_str,
                    self.__hash_2_sql(field),
                    bit_array_indexes_str,
                    self.__hash_3_sql(field),
                    bit_array_indexes_str)

    def add(self, v):

        v1_hash_1 = self.__hash_1(v)
        self.__ensure_set(v1_hash_1)
        v1_hash_2 = self.__hash_2(v)
        self.__ensure_set(v1_hash_2)
        v1_hash_3 = self.__hash_3(v)
        self.__ensure_set(v1_hash_3)
