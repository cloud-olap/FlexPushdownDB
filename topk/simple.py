# -*- coding: utf-8 -*-
"""Simple top k query

Performs a full "table" load and simply stops iterating through the response once the desired
record count limit has been reached. This is intended to demonstrate the worst case/most naive
method of loading of records.

"""

from sql.cursor import Cursor

LIMIT = 500

cur = Cursor()\
    .select('customer.csv')

try:
    rows = cur.execute()

    i = 0
    for r in rows:
        i += 1
        print("Row {}: {}".format(i, r))
        if i >= LIMIT:
            break

finally:
    cur.close()
