# -*- coding: utf-8 -*-
"""Bloom filter hash function support

"""

import random
import primesieve


class UniversalSQLHashFunction(object):
    """An implementation of a universal hash function (Carter and Wegman) for use in a bloom filter where a variable
    number of hash functions with good hashing properties are required and can be expressed as a SQL expression using
    only the operators *, +, / and % (which are the subset of arithmetical operators supported by s3 select)

    TODO: Implement this for strings

    """

    def __init__(self, m):
        self.m = m

        # Pick a random integer between m and 2m
        i = random.randint(m, 2 * m)
        # Get the nth prime greater than i
        self.p = primesieve.nth_prime(i)

        # Set a = random int (less than p) mod p (where a != 0)
        self.a = random.randint(1, self.p - 1) % self.p

        # Set b = random int (less than p) mod p
        self.b = random.randint(0, self.p - 1) % self.p

    def __call__(self, *args, **kwargs):
        return self.__eval(args[0])

    def __eval(self, k):
        return self.__hash_(self.a, k, self.b, self.p, self.m)

    def sql(self, kf):
        return self.__hash_sql(self.a, kf, self.b, self.p, self.m)

    @staticmethod
    def __hash_(a, x, b, p, m):
        """Carter and Wegmans integer universal hashing formula.

        :param a: random int (less than p) mod p (where a != 0)
        :param x: the key to hash
        :param b: random int (less than p) mod p
        :param p: prime > m and < 2m
        :param m: number of bins
        :return: Hash value
        """

        return ((a * x + b) % p) % m

    @staticmethod
    def __hash_sql(a, xf, b, p, m):
        """Carter and Wegmans integer universal hashing formula as a SQL expression.

        :param a: random int (less than p) mod p (where a != 0)
        :param xf: the field name containing the key to hash
        :param b: random int (less than p) mod p
        :param p: prime > m and < 2m
        :param m: number of bins
        :return: Hash SQL expression
        """

        return "( ( {} * cast({} as int) + {} ) % {} ) % {}" \
            .format(a, xf, b, p, m)
