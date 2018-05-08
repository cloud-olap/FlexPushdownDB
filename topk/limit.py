# -*- coding: utf-8 -*-
"""Limit top k query

Performs a partial "table" load by using the LIMIT keyword in S3 select. Note that the documentation at
https://docs.aws.amazon.com/AmazonS3/latest/dev/s3-glacier-select-sql-reference-select.html#s3-glacier-select-sql-reference-limit
does not specify precisely what limit does. Experimentation indicates it returns a number of rows less than the number
specified in the limit clause.

"""

from sql.cursor import Cursor
from sql.cursor import LimitStrategy

LIMIT = 500

cur = Cursor()\
    .select('customer.csv')\
    .limit(LIMIT, LimitStrategy.OP)

rows = cur.execute()

for r in rows:
    print(r)

cur.close()
