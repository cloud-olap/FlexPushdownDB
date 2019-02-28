# -*- coding: utf-8 -*-
from py._log import warning

import s3filter.util.constants
from s3filter.sql.format import Format


def get_file_key(file_, sharded, shard=None, sf=None, format_=None):

    if sf is None:
        sf = s3filter.util.constants.TPCH_SF

    if format_ is Format.CSV:
        object_path = ""
    elif format_ is Format.PARQUET:
        object_path = "tpch-parquet/"
    else:
        raise Exception("Unrecognized format {}".format(format_))

    if sf == 1:

        if format_ is Format.CSV:
            format_str = "csv"
        elif format_ is Format.PARQUET:
            format_str = "snappy.parquet"
        else:
            raise Exception("Unrecognized format {}".format(format_))

        if sharded:
            if file_ in ['lineitem', 'customer', 'orders']:
                return '{}tpch-sf1/{}_sharded/{}.{}.{}'.format(object_path, file_, file_, format_str, shard)
            else:
                raise Exception()
        else:
            return '{}tpch-sf1/{}.{}'.format(object_path, file_, format_str)
    elif sf == 10:

        if format_ is Format.CSV:
            format_str = "tbl"
        elif format_ is Format.PARQUET:
            format_str = "snappy.parquet"
        else:
            raise Exception("Unrecognized format {}".format(format_))

        if sharded:
            return '{}tpch-sf10/{}_sharded/{}.{}.{}'.format(object_path, file_, file_, format_str, shard + 1)
        else:
            return '{}tpch-sf10/{}.{}'.format(object_path, file_, format_str)
    elif sf == 100:

        if format_ is Format.CSV:
            format_str = "csv"
        elif format_ is Format.PARQUET:
            format_str = "snappy.parquet"
        else:
            raise Exception("Unrecognized format {}".format(format_))

        if sharded:
            return '{}tpch-sf100/{}_sharded/{}.{}.{}'.format(object_path, file_, file_, shard + 1, format_str)
        else:
            return '{}tpch-sf100/{}.{}'.format(object_path, file_, format_str)
    else:
        raise Exception()
