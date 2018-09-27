# -*- coding: utf-8 -*-
"""Operator messages

"""
from s3filter.multiprocessing.message_base import MessageBase
from s3filter.multiprocessing.message_base_type import MessageBaseType
from s3filter.op.tuple import Tuple
from s3filter.hash.sliced_bloom_filter import SlicedBloomFilter
from s3filter.hash.scalable_bloom_filter import ScalableBloomFilter


class TupleMessage(MessageBase):
    """Message containing a tuple.

    """

    def __init__(self, sender_name, data):
        """Creates a new TupleMessage

        :param tuple_: The tuple content of the message
        """

        super(TupleMessage, self).__init__(MessageBaseType.tuple, sender_name, data)

        if type(data) is not Tuple:
            raise Exception("Message content type is {}. Type must be Tuple".format(type(data)))

        self.data = data
        self.tuple_ = data


class BloomMessage(object):
    """Message containing a bloom filter.

    """

    def __init__(self, bloom_filter):
        """Creates a new BloomMessage

        :param bloom_filter: The bloom filter content of the message
        """

        if type(bloom_filter) is not ScalableBloomFilter and \
                type(bloom_filter) is not SlicedBloomFilter:
            raise Exception("Message content type is {}. Type must be '{}', or '{}'"
                            .format(type(bloom_filter),
                                    SlicedBloomFilter.__class__.__name__,
                                    ScalableBloomFilter.__class__.__name__))

        self.bloom_filter = bloom_filter


class HashTableMessage(object):

    def __init__(self, hashtable):
        self.hashtable = hashtable

class StringMessage(object):
    """Message containing a string.

    """

    def __init__(self, string_):
        """Creates a new TupleMessage

        :param string_: The string content of the message
        """

        if type(string_) is not str:
            raise Exception("Message content type is {}. Type must be str".format(type(string_)))

        self.string_ = string_
