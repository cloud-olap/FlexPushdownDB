# -*- coding: utf-8 -*-
"""SQL function support

"""
import numbers
from datetime import datetime

from util.datetime_util import str_to_millis, dt_to_millis

timestamp = 'type_timestamp'


def cast(ex, t):
    if t == timestamp:
        if type(ex) == str:
            return str_to_millis(ex)
        elif type(ex) == datetime:
            return dt_to_millis(ex)
        else:
            raise Exception("Unrecognized type {}".format(t))
    else:
        raise Exception('Unrecognized type {}'.format(t))


def sum_fn(ex, ctx):

    if not isinstance(ex, numbers.Number):
        raise Exception("Illegal expression type {} for expression {}. Sum expression must be numeric".format(type(ex), ex))

    current_sum = ctx.result

    current_sum += ex

    ctx.result = current_sum


def avg_fn(ex, ctx):

    if not isinstance(ex, numbers.Number):
        raise Exception(
            "Illegal expression type {} for expression {}. Sum expression must be numeric".format(type(ex), ex))

    current_count = ctx.vars_.get('.count', 0)
    # current_total = ctx.vars_.get('.total', 0)
    current_running_avg = ctx.result

    current_count += 1
    # current_total += ex
    current_running_avg = (current_running_avg * (float(current_count - 1)) + ex) / float(current_count)

    ctx.vars_['.count'] = current_count
    # ctx.vars_['.total'] = current_total

    # current_avg = current_total / float(current_count)
    # if current_running_avg is not current_avg:
    #     raise Exception("Running average {} should equal average {}".format(current_running_avg, current_avg))

    ctx.result = current_running_avg


def count_fn(_ex, ctx):

    current_count = ctx.result

    current_count += 1

    ctx.result = current_count

