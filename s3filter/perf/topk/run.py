# -*- coding: utf-8 -*-
"""Limit top k query runner

Runs the top k query tests

"""

from s3filter.perf.topk import simple, limit


def main():
    limit.main()
    simple.main()


if __name__ == "__main__":
    main()
