# -*- coding: utf-8 -*-


def get_file_key(file_, sharded, shard=None):

    if sharded:
        if file_ == 'lineitem':
            return 'sf1000-lineitem/lineitem_{}.csv'.format(shard)
        elif file_ == 'part':
            return '{}.csv'.format(file_)  # Not sharded yet
        else:
            raise Exception()
    else:
        return '{}.csv'.format(file_)
