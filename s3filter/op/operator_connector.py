# -*- coding: utf-8 -*-
"""Utilities for connecting operators


"""

from s3filter.op.hash_join_build import HashJoinBuild
from s3filter.op.hash_join_probe import HashJoinProbe


def do_connect(from_op, to_op):
    if type(from_op) is HashJoinBuild and type(to_op) is HashJoinProbe:
        to_op.connect_build_producer(from_op)
    elif type(from_op) is not HashJoinBuild and type(to_op) is HashJoinProbe:
        to_op.connect_tuple_producer(from_op)
    else:
        from_op.connect(to_op)


def connect_many_to_many(from_ops, to_ops):
    """Tries to connect from ops to to ops with some awareness of partitions.

    :param from_ops:
    :param to_ops:
    :return:
    """

    if len(from_ops) > len(to_ops):
        map(lambda (p, o): do_connect(o, to_ops[p % len(to_ops)]), enumerate(from_ops))
    elif len(from_ops) == len(to_ops):
        map(lambda (p, o): do_connect(o, to_ops[p]), enumerate(from_ops))
    else:
        map(lambda (p, o): do_connect(from_ops[p % len(from_ops)], o), enumerate(to_ops))


def connect_many_to_one(from_ops, to_op):
    map(lambda (p, o): do_connect(o, to_op), enumerate(from_ops))


def connect_one_to_one(from_op, to_op):
    from_op.connect(to_op)


def connect_all_to_all(from_ops, to_ops):
    map(lambda (p1, o1): map(lambda (p2, o2): do_connect(o1, o2), enumerate(to_ops)), enumerate(from_ops))


def connect_one_to_many(from_op, to_ops):
    map(lambda (p2, o2): do_connect(from_op, o2), enumerate(to_ops))
