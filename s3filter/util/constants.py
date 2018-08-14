# -*- coding: utf-8 -*-
"""
Place for constants

"""

TPCH_SF = 1
S3_BUCKET_NAME = 's3filter'
TABLE_STORAGE_LOC = 'tables/{}/'.format(S3_BUCKET_NAME)
BYTE_TO_GB = 1 / (1024 * 1024 * 1024.0)
BYTE_TO_MB = 1 / (1024 * 1024.0)
SEC_TO_HOUR = 1 / 3600.0
