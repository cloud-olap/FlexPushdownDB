# -*- coding: utf-8 -*-
import s3filter.util.constants

def get_file_key(file_, sharded, shard=None, sf=None):
    if sf == None:
        sf = s3filter.util.constants.TPCH_SF
    if sf == 1:
        if sharded:
            if file_ == 'lineitem':
                return 'tpch-sf1/lineitem_sharded/lineitem.csv.{}'.format(shard)
            elif file_ == 'part':
                return '{}.csv'.format(file_)  # Not sharded yet
            else:
                raise Exception()
        else:
            return 'tpch-sf1/{}.csv'.format(file_)
    elif sf == 10:
        if sharded:
            return 'tpch-sf10/{}_sharded/{}.tbl.{}'.format(file_, file_, shard + 1)
        else:
            return 'tpch-sf10/{}.tbl'.format(file_)
    elif sf == 100:
        if sharded:
            return 'tpch-sf100/{}_sharded/{}.csv.{}'.format(file_, file_, shard + 1)
        else:
            return 'tpch-sf100/{}.csv'.format(file_)
    else:
        raise Exception()


