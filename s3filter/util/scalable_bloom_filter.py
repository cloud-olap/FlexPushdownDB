# -*- coding: utf-8 -*-
"""Scalable bloom filter support

"""

from s3filter.util.bloom_filter import BloomFilter


class ScalableBloomFilter(object):
    """Bloom filter implementation based off the scalable bloom filter described by
    Almeida, Baquero, Preguiça, Hutchison.

    Keeps creating new bloom filters as more capacity is required.

    Inspired by pybloom library

    """

    SMALL_SET_GROWTH = 2  # slower, but takes up less memory
    LARGE_SET_GROWTH = 4  # faster, but takes up more memory faster

    def __init__(self, initial_capacity, error_rate, mode):

        if not initial_capacity > 0:
            raise Exception("Capacity must be > 0")
        if not error_rate or error_rate < 0:
            raise Exception("Error_Rate must be a decimal less than 0.")

        self.scale = mode
        self.ratio = 0.9
        self.initial_capacity = initial_capacity
        self.error_rate = error_rate
        self.filters = []

    def __contains__(self, key):
        """ Tests if the key is in the filter

        :param key: The key to tst
        :return: True or False
        """

        for f in reversed(self.filters):
            if key in f:
                return True

        return False

    def add(self, key):
        """Adds the key increasing capacity if needed

        :param key: The key to add
        :return: Whether the key was already added
        """

        if key in self:
            return True

        if not self.filters:
            filter_ = BloomFilter(
                capacity=self.initial_capacity,
                error_rate=self.error_rate * self.ratio)
            self.filters.append(filter_)
        else:
            filter_ = self.filters[-1]
            if filter_.count >= filter_.capacity:
                filter_ = BloomFilter(
                    capacity=filter_.capacity * self.scale,
                    error_rate=filter_.error_rate * self.ratio)
                self.filters.append(filter_)

        filter_.add(key)

        return False

    @property
    def capacity(self):
        """Returns the total capacity for all filters

        :return: Capacity
        """

        return sum(f.capacity for f in self.filters)

    @property
    def count(self):
        """Size of filters

        :return: Size
        """

        return len(self)

    def __len__(self):
        """Returns the total number of elements stored

        :return: Number of elements
        """

        return sum(f.count for f in self.filters)

    def sql_predicate(self, field):
        """SQL expression representing a predicate that can be used to push down the bloom filter into an SQL statement.

        :param field: The name of the field (column) containing the key data we want to tst against
        :return: SQL string
        """

        sql = ""

        for i in range(0, len(self.filters)):
            f = self.filters[i]
            sql += f.sql_predicate(field)
            if i < len(self.filters) - 1:
                sql += " or "

        return sql
