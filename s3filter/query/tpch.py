# -*- coding: utf-8 -*-
import s3filter.util.constants

def get_file_key(file_, sharded, shard=None):
    if s3filter.util.constants.TPCH_SF == 1: 
        if sharded:
            if file_ == 'lineitem':
                return 'sf1000-lineitem/lineitem_{}.csv'.format(shard)
            elif file_ == 'part':
                return '{}.csv'.format(file_)  # Not sharded yet
            else:
                raise Exception()
        else:
            return '{}.csv'.format(file_)
    elif s3filter.util.constants.TPCH_SF == 10: 
        if sharded:
            return 'tpch-sf10/{}_sharded/{}.tbl.{}'.format(file_, file_, shard + 1)
        else:
            return 'tpch-sf10/{}.tbl'.format(file_)
    else:
        raise Exception()

