# -*- coding: utf-8 -*-
"""Actor tests

"""
import dask
from dask.distributed import Client, LocalCluster


def test_dask():
    cluster = LocalCluster()
    client = Client(cluster)

    def load1(shard):
        print("load1 {}".format(shard))

        tuples = []
        for i in range(1, 5):
            tuples.append(["load1_1_{}".format(shard), "load1_1_{}".format(shard), 1])

        return tuples

    def load2(shard):
        print("load2 {}".format(shard))
        tuples = []
        for i in range(1, 5):
            tuples.append(["load2_1_{}".format(shard), "load2_1_{}".format(shard), 1])

        return tuples

    def project(tuples):
        print("project {}".format(tuples))
        return map(lambda t: [t[0], t[1], t[2]], tuples)

    def join(tuples1, tuples2):
        print("join {}, {}".format(tuples1, tuples2))
        return map(lambda t: tuples1[0] + tuples2[0], tuples1)

    def agg(tuples):
        print("agg {}".format(tuples))
        pr = map(lambda t: t[2], tuples)
        return sum(pr)

    def reduce(xs):
        print("reduce {}".format(xs))
        return sum(xs)

    data = [1, 2, 3, 4, 5]

    tuples = []
    for shard in data:
        tuples1 = dask.delayed(load1)(shard)
        tuples2 = dask.delayed(load2)(shard)
        projected_tuples1 = dask.delayed(project)(tuples1)
        projected_tuples2 = dask.delayed(project)(tuples2)
        joined_tuples = dask.delayed(join)(projected_tuples1, projected_tuples2)
        aggs = dask.delayed(agg)(joined_tuples)
        tuples.append(aggs)

    total = dask.delayed(reduce)(tuples)

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
