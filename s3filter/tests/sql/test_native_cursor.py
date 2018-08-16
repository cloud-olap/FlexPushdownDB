# -*- coding: utf-8 -*-
"""Tests for some select edge cases

"""
import pandas
import scan

def test_scan():

    # status = scan.stuff("ls -l")

    print("Executing")

    res = scan.execute("lineitem.csv", "select * from s3Object limit 10000")

    print("Executed. Result is:")
    print(res)

    df = pandas.DataFrame(res)

    print(df)


if __name__ == "__main__":
    test_scan()
