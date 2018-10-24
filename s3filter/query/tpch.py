# -*- coding: utf-8 -*-
from py._log import warning

import s3filter.util.constants


def get_file_key(file_, sharded, shard=None, sf=None):
    if sf is None:
        sf = s3filter.util.constants.TPCH_SF
    if sf == 1:
        if sharded:
            if file_ in ['lineitem', 'customer', 'orders']:
                return 'tpch-sf1/{}_sharded/{}.csv.{}'.format(file_, file_, shard)
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
