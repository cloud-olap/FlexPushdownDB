# -*- coding: utf-8 -*-

from s3filter.op.filter import Filter
from s3filter.op.predicate_expression import PredicateExpression


def filter_def():
    return Filter(
        PredicateExpression(lambda t_:
                            (
                                    t_['p_brand'] == 'Brand#11' and
                                    t_['p_container'] in ['SM CASE', 'SM BOX', 'SM PACK', 'SM PKG'] and
                                    3 <= int(t_['l_quantity']) <= 3 + 10 and
                                    1 <= int(t_['p_size']) <= 5 and
                                    t_['l_shipmode'] in ['AIR', 'AIR REG'] and
                                    t_['l_shipinstruct'] == 'DELIVER IN PERSON'
                            ) or (
                                    t_['p_brand'] == 'Brand#44' and
                                    t_['p_container'] in ['MED BAG', 'MED BOX', 'MED PACK', 'MED PKG'] and
                                    16 <= int(t_['l_quantity']) <= 16 + 10 and
                                    1 <= int(t_['p_size']) <= 10 and
                                    t_['l_shipmode'] in ['AIR', 'AIR REG'] and
                                    t_['l_shipinstruct'] == 'DELIVER IN PERSON'
                            ) or (
                                    t_['p_brand'] == 'Brand#53' and
                                    t_['p_container'] in ['LG CASE', 'LG BOX', 'LG PACK', 'LG PKG'] and
                                    24 <= int(t_['l_quantity']) <= 24 + 10 and
                                    1 <= int(t_['p_size']) <= 15 and
                                    t_['l_shipmode'] in ['AIR', 'AIR REG'] and
                                    t_['l_shipinstruct'] == 'DELIVER IN PERSON'
                            )),
        'filter',
        False)
