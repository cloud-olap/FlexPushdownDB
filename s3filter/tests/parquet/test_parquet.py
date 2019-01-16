import os
from os import mkdir

import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from boto3 import Session
from botocore.config import Config

from s3filter import ROOT_DIR
from s3filter.sql.pandas_cursor import PandasCursor


def test_read_parquet_obj_from_csv_file():
    table = read_parquet_from_csv_file('supplier.csv')
    df = arrow_to_pandas(table)

    assert_supplier_table(df, 10000, use_ordinal_columns=False)


def assert_supplier_table(df, num_rows, use_ordinal_columns=False):
    assert len(df) == num_rows

    if not use_ordinal_columns:
        assert df.iloc[0]['s_suppkey'] == "1"
        assert df.iloc[0]['s_name'] == "Supplier#000000001"
        assert df.iloc[0]['s_address'] == " N kD4on9OM Ipw3,gf0JBoQDd7tgrzrddZ"
        assert df.iloc[0]['s_nationkey'] == "17"
        assert df.iloc[0]['s_phone'] == "27-918-335-1736"
        assert df.iloc[0]['s_acctbal'] == "5755.94"
        assert df.iloc[0]['s_comment'] == "each slyly above the careful"
    else:
        assert df.iloc[0]['_0'] == "1"
        assert df.iloc[0]['_1'] == "Supplier#000000001"
        assert df.iloc[0]['_2'] == " N kD4on9OM Ipw3,gf0JBoQDd7tgrzrddZ"
        assert df.iloc[0]['_3'] == "17"
        assert df.iloc[0]['_4'] == "27-918-335-1736"
        assert df.iloc[0]['_5'] == "5755.94"
        assert df.iloc[0]['_6'] == "each slyly above the careful"


def arrow_to_pandas(table):
    return table.to_pandas()


def read_parquet_from_csv_file(csv_file):
    df = pd.read_csv(csv_file, sep='|', dtype=str)
    table = pandas_to_arrow(df)
    return table


def pandas_to_arrow(df):
    cols = df.columns.values.tolist()
    s_cols = map(lambda x: (x, pa.string()), cols)
    schema = pa.schema(s_cols)
    table = pa.Table.from_pandas(df, schema=schema, preserve_index=False, nthreads=4)
    return table


def convert_csv_file_to_parquet_file(csv_file, parquet_file):
    table = read_parquet_from_csv_file(csv_file)
    pq.write_table(table, parquet_file)


def test_convert_csv_file_to_parquet_file():
    ensure_dir("{}/../build/tests/".format(ROOT_DIR))

    parquet_file = "{}/../build/tests/supplier.parquet".format(ROOT_DIR)

    convert_csv_file_to_parquet_file('supplier.csv', parquet_file)

    table = pq.read_table(parquet_file)
    df = arrow_to_pandas(table)

    assert_supplier_table(df, 10000, use_ordinal_columns=False)


def test_write_parquet_file_to_s3():
    ensure_dir("{}/../build/tests/".format(ROOT_DIR))

    upload_parquet_file = "{}/../build/tests/supplier.upload.parquet".format(ROOT_DIR)
    download_parquet_file = "{}/../build/tests/supplier.download.parquet".format(ROOT_DIR)

    convert_csv_file_to_parquet_file('supplier.csv', upload_parquet_file)

    cfg = Config(region_name="us-east-1", parameter_validation=False, max_pool_connections=10)
    session = Session()
    s3 = session.client('s3', config=cfg)

    s3.upload_file(upload_parquet_file, 's3filter', 'parquet/supplier.parquet')

    s3.download_file('s3filter', 'parquet/supplier.parquet', download_parquet_file)

    table = pq.read_table(download_parquet_file)
    df = arrow_to_pandas(table)

    assert_supplier_table(df, 10000, use_ordinal_columns=False)


def ensure_dir(path):
    if not os.path.exists(path):
        os.mkdir(path)


def test_s3_select_from_parquet():
    cfg = Config(region_name="us-east-1", parameter_validation=False, max_pool_connections=10)
    session = Session()
    s3 = session.client('s3', config=cfg)

    response = s3.select_object_content(Bucket='s3filter', Key='parquet/supplier.parquet',
                                        Expression='select * from s3Object', ExpressionType='SQL',
                                        InputSerialization={
                                            'CompressionType': 'NONE',
                                            'Parquet': {}

                                        },
                                        OutputSerialization={
                                            'CSV': {}
                                        })

    df = None

    cursor = PandasCursor(None)
    cursor.event_stream = response['Payload']
    dfs = cursor.parse_event_stream()

    for partial_df in dfs:
        if df is None:
            df = partial_df
        else:
            df = pd.concat(df, partial_df)

    assert_supplier_table(df, 10000, use_ordinal_columns=True)


def test_filtered_s3_select_from_parquet():
    cfg = Config(region_name="us-east-1", parameter_validation=False, max_pool_connections=10)
    session = Session()
    s3 = session.client('s3', config=cfg)

    response = s3.select_object_content(Bucket='s3filter', Key='parquet/supplier.parquet',
                                        Expression='select * from s3Object where cast(s_acctbal as float) > 500.0 ',
                                        ExpressionType='SQL',
                                        InputSerialization={
                                            'CompressionType': 'NONE',
                                            'Parquet': {}
                                        },
                                        OutputSerialization={
                                            'CSV': {}
                                        })

    df = None

    cursor = PandasCursor(None)
    cursor.event_stream = response['Payload']
    dfs = cursor.parse_event_stream()

    for partial_df in dfs:
        if df is None:
            df = partial_df
        else:
            df = pd.concat(df, partial_df)

    assert_supplier_table(df, 8642, use_ordinal_columns=True)
