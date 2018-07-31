# -*- coding: utf-8 -*-
"""Actor tests

"""
import dask
from dask.distributed import Client, LocalCluster


def test_dask():
    _cluster = LocalCluster()
    _client = Client(_cluster)

    def load1(shard):
        print("load1 {}".format(shard))

        ts = []
        for i in range(1, 5):
            ts.append(["load1_1_{}".format(shard), "load1_1_{}".format(shard), 1])

        return ts

    def load2(shard):
        print("load2 {}".format(shard))
        ts = []
        for i in range(1, 5):
            ts.append(["load2_1_{}".format(shard), "load2_1_{}".format(shard), 1])

        return ts

    def project(ts):
        print("project {}".format(ts))
        return map(lambda t: [t[0], t[1], t[2]], ts)

    def join(ts1, ts2):
        print("join {}, {}".format(ts1, ts2))
        return map(lambda t: ts1[0] + ts2[0], ts1)

    def agg(ts):
        print("agg {}".format(ts))
        pr = map(lambda t: t[2], ts)
        return sum(pr)

    def reduce_(xs):
        print("reduce {}".format(xs))
        return sum(xs)

    data = [1, 2, 3, 4, 5]

    tuples = []
    for s in data:
        tuples1 = dask.delayed(load1)(s)
        tuples2 = dask.delayed(load2)(s)
        projected_tuples1 = dask.delayed(project)(tuples1)
        projected_tuples2 = dask.delayed(project)(tuples2)
        joined_tuples = dask.delayed(join)(projected_tuples1, projected_tuples2)
        aggs = dask.delayed(agg)(joined_tuples)
        tuples.append(aggs)

    total = dask.delayed(reduce_)(tuples)

    # output = []
    # for x in data:
    #     a = dask.delayed(inc)(x)
    #     for y in a:
    #         b = dask.delayed(double)(y)
    #         c = dask.delayed(add)(a, b)
    #     output.append(c)
    #
    # total = dask.delayed(sum)(output)

    total.visualize()

    res = total.compute()

    print(res)

    _client.close()
    _cluster.close()


