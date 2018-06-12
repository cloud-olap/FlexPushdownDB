# -*- coding: utf-8 -*-
"""Exploring how to write to s3 in an s3 select compatible way

"""
import boto3
from s3filter.util import constants
from s3filter.sql.cursor import Cursor

s3 = boto3.client('s3')

# Write the sample csv data
f = open('./write.csv', 'rb')
s3.upload_fileobj(f, constants.S3_BUCKET_NAME, 'write.csv')
f.close()

# Read it back out
cur = Cursor()\
    .select('write.csv', 'select * from S3Object;')

rows = cur.execute()

for r in rows:
    print(r)

cur.close()




