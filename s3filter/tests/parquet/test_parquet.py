import os
import shutil

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from boto3 import Session
from botocore.config import Config

from s3filter import ROOT_DIR
from s3filter.sql.pandas_cursor import PandasCursor

tests_path = os.path.abspath("{}/../build/tests".format(ROOT_DIR))


def test_read_parquet_obj_from_csv_file():
    table = read_parquet_from_csv_file('{}/tests/parquet/supplier.csv'.format(ROOT_DIR))
    df = arrow_to_pandas(table)

    assert_supplier_table(df, 10000, use_ordinal_columns=False)


def assert_supplier_table(df, num_rows, use_ordinal_columns=False):
    assert len(df) == num_rows

    if not use_ordinal_columns:

        rows = df[df['s_suppkey'] == '1']

        assert len(rows) == 1

        row = rows.iloc[0]

        assert row['s_suppkey'] == "1"
        assert row['s_name'] == "Supplier#000000001"
        assert row['s_address'] == " N kD4on9OM Ipw3,gf0JBoQDd7tgrzrddZ"
        assert row['s_nationkey'] == "17"
        assert row['s_phone'] == "27-918-335-1736"
        assert row['s_acctbal'] == "5755.94"
        assert row['s_comment'] == "each slyly above the careful"
    else:

        rows = df[df['_0'] == '1']

        assert len(rows) == 1

        row = rows.iloc[0]

        assert row['_0'] == "1"
        assert row['_1'] == "Supplier#000000001"
        assert row['_2'] == " N kD4on9OM Ipw3,gf0JBoQDd7tgrzrddZ"
        assert row['_3'] == "17"
        assert row['_4'] == "27-918-335-1736"
        assert row['_5'] == "5755.94"
        assert row['_6'] == "each slyly above the careful"


def arrow_to_pandas(table):
    df = table.to_pandas()
    df = df.astype(str)
    return df


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


# def convert_csv_file_to_parquet_file(csv_file, parquet_file):
#     table = read_parquet_from_csv_file(csv_file)
#     pq.write_table(table, parquet_file)


def test_convert_csv_file_to_parquet_file():
    test_path = tests_path + "/{}".format(test_convert_csv_file_to_parquet_file.__name__)
    ensure_dir(test_path)

    parquet_file = "{}/supplier.parquet".format(test_path)

    convert_csv_file_to_parquet_file('{}/tests/parquet/supplier.csv'.format(ROOT_DIR), parquet_file)

    table = pq.read_table(parquet_file)
    df = arrow_to_pandas(table)

    assert_supplier_table(df, 10000, use_ordinal_columns=False)


def test_write_parquet_file_to_s3():
    test_path = tests_path + "/{}".format(test_write_parquet_file_to_s3.__name__)
    ensure_dir(test_path)

    upload_parquet_file = "{}/supplier.upload.parquet".format(test_path)
    download_parquet_file = "{}/supplier.download.parquet".format(test_path)

    convert_csv_file_to_parquet_file('{}/tests/parquet/supplier.csv'.format(ROOT_DIR), upload_parquet_file)

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
        os.makedirs(path)


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


def test_write_partitioned_parquet():
    test_path = tests_path + "/{}".format(test_write_partitioned_parquet.__name__)
    ensure_dir(test_path)

    parquet_path = "{}/supplier.parquet".format(test_path)

    if os.path.exists(parquet_path):
        shutil.rmtree(parquet_path)

    convert_csv_file_to_parquet_dataset('{}/tests/parquet/supplier.csv'.format(ROOT_DIR), parquet_path, ['s_nationkey'])

    dataset = pq.ParquetDataset(parquet_path, metadata_nthreads=4)
    table = dataset.read()
    df = arrow_to_pandas(table)

    assert_supplier_table(df, 10000, use_ordinal_columns=False)


def convert_csv_file_to_parquet_dataset(csv_file, parquet_path, partition_cols):
    table = read_parquet_from_csv_file(csv_file)
    pq.write_to_dataset(table,
                        root_path=parquet_path,
                        partition_cols=partition_cols,
                        preserve_index=False)


def test_write_partitioned_parquet_to_s3():
    test_path = tests_path + "/{}".format(test_write_partitioned_parquet_to_s3.__name__)
    ensure_dir(test_path)

    upload_parquet_path = "{}/supplier.upload.parquet".format(test_path)

    if os.path.exists(upload_parquet_path):
        shutil.rmtree(upload_parquet_path)

    convert_csv_file_to_parquet_dataset('{}/tests/parquet/supplier.csv'.format(ROOT_DIR), upload_parquet_path,
                                        ['s_nationkey'])

    cfg = Config(region_name="us-east-1", parameter_validation=False, max_pool_connections=10)
    session = Session()
    s3 = session.client('s3', config=cfg)

    # s3.upload_file(upload_parquet_file, 's3filter', 'parquet/supplier.parquet')

    remote_path = "/supplier.partitioned.parquet"

    for dirpath, dirnames, filenames in os.walk(test_path):
        for filename in filenames:
            local_path = os.path.join(dirpath, filename)
            relative_path = os.path.relpath(local_path, upload_parquet_path)
            s3_path = os.path.join(remote_path, relative_path)
            s3.upload_file(local_path, 's3filter', "parquet{}".format(s3_path))


def test_s3_select_from_partitioned_parquet():
    cfg = Config(region_name="us-east-1", parameter_validation=False, max_pool_connections=10)
    session = Session()
    s3 = session.client('s3', config=cfg)

    with pytest.raises(Exception):
        s3.select_object_content(Bucket='s3filter', Key='parquet/supplier.partitioned.parquet',
                                 Expression='select * from s3Object', ExpressionType='SQL',
                                 InputSerialization={
                                     'CompressionType': 'NONE',
                                     'Parquet': {}

                                 },
                                 OutputSerialization={
                                     'CSV': {}
                                 })


def test_projected_s3_select_from_parquet():
    cfg = Config(region_name="us-east-1", parameter_validation=False, max_pool_connections=10)
    session = Session()
    s3 = session.client('s3', config=cfg)

    response = s3.select_object_content(Bucket='s3filter', Key='parquet/supplier.parquet',
                                        Expression='select s_suppkey from s3Object',
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

    assert len(df) == 10000

    rows = df[df['_0'] == '1']

    assert len(rows) == 1

    row = rows.iloc[0]

    assert row['_0'] == "1"


def test_projected_vs_all_s3_select_from_parquet():
    cfg = Config(region_name="us-east-1", parameter_validation=False, max_pool_connections=10)
    session = Session()
    s3 = session.client('s3', config=cfg)

    response1 = s3.select_object_content(Bucket='s3filter', Key='parquet/supplier.parquet',
                                         Expression='select s_suppkey from s3Object',
                                         ExpressionType='SQL',
                                         InputSerialization={
                                             'CompressionType': 'NONE',
                                             'Parquet': {}

                                         },
                                         OutputSerialization={
                                             'CSV': {}
                                         })

    df1 = None

    cursor1 = PandasCursor(None)
    cursor1.event_stream = response1['Payload']
    dfs1 = cursor1.parse_event_stream()

    for partial_df1 in dfs1:
        if df1 is None:
            df1 = partial_df1
        else:
            df1 = pd.concat(df1, partial_df1)

    response2 = s3.select_object_content(Bucket='s3filter', Key='parquet/supplier.parquet',
                                         Expression='select * from s3Object',
                                         ExpressionType='SQL',
                                         InputSerialization={
                                             'CompressionType': 'NONE',
                                             'Parquet': {}

                                         },
                                         OutputSerialization={
                                             'CSV': {}
                                         })

    df2 = None

    cursor2 = PandasCursor(None)
    cursor2.event_stream = response2['Payload']
    dfs2 = cursor2.parse_event_stream()

    for partial_df2 in dfs2:
        if df2 is None:
            df2 = partial_df2
        else:
            df2 = pd.concat(df2, partial_df2)

    assert len(df1) == 10000
    assert len(df2) == 10000

    rows1 = df1[df1['_0'] == '1']
    rows2 = df2[df2['_0'] == '1']

    assert len(rows1) == 1
    assert len(rows2) == 1

    row1 = rows1.iloc[0]
    row2 = rows2.iloc[0]

    assert row1['_0'] == "1"
    assert row2['_0'] == "1"

    assert cursor2.bytes_scanned > cursor1.bytes_scanned


def test_write_large_parquet_file():
    num_writes = 500

    test_path = tests_path + "/{}".format(test_write_large_parquet_file.__name__)
    ensure_dir(test_path)

    parquet_file = "{}/supplier.parquet".format(test_path)

    convert_csv_file_to_parquet_file('{}/tests/parquet/supplier.csv'.format(ROOT_DIR), parquet_file, 'none', num_writes)

    table = pq.read_table(parquet_file)
    df = arrow_to_pandas(table)

    assert len(df) == 10000 * num_writes

    rows = df[df['s_suppkey'] == '1']

    assert len(rows) == num_writes

    row = rows.iloc[0]

    assert row['s_suppkey'] == "1"


def convert_csv_file_to_parquet_file(csv_file, parquet_file, compression='none', ntimes=1, use_dictionary=False,
                                     version='1.0'):
    table = read_parquet_from_csv_file(csv_file)
    pqwriter = pq.ParquetWriter(parquet_file, version=version, schema=table.schema, compression=compression,
                                use_dictionary=use_dictionary)
    for i in range(0, ntimes):
        pqwriter.write_table(table)
    pqwriter.close()


def convert_csv_file_to_typed_parquet_file(csv_file, parquet_file, compression, schema, ntimes=1):
    table = read_parquet_from_csv_file(csv_file)
    table = table.cast(schema)
    pqwriter = pq.ParquetWriter(parquet_file, schema=schema, compression=compression)
    for i in range(0, ntimes):
        pqwriter.write_table(table)
    pqwriter.close()


def test_write_large_parquet_file_to_s3():
    num_writes = 500

    test_path = tests_path + "/{}".format(test_write_large_parquet_file_to_s3.__name__)
    ensure_dir(test_path)

    upload_parquet_file = "{}/supplier.upload.parquet".format(test_path)
    download_parquet_file = "{}/supplier.download.parquet".format(test_path)

    convert_csv_file_to_parquet_file('{}/tests/parquet/supplier.csv'.format(ROOT_DIR),
                                     upload_parquet_file,
                                     'none',
                                     num_writes)

    cfg = Config(region_name="us-east-1", parameter_validation=False, max_pool_connections=10)
    session = Session()
    s3 = session.client('s3', config=cfg)

    s3.upload_file(upload_parquet_file, 's3filter', 'parquet/supplier.large.parquet')

    s3.download_file('s3filter', 'parquet/supplier.large.parquet', download_parquet_file)

    table = pq.read_table(download_parquet_file)
    df = arrow_to_pandas(table)

    assert len(df) == 10000 * num_writes

    rows = df[df['s_suppkey'] == '1']

    assert len(rows) == num_writes

    row = rows.iloc[0]

    assert row['s_suppkey'] == "1"


def test_write_large_typed_parquet_file_to_s3():
    num_writes = 500

    test_path = tests_path + "/{}".format(test_write_large_typed_parquet_file_to_s3.__name__)
    ensure_dir(test_path)

    upload_parquet_file = "{}/supplier.upload.parquet".format(test_path)
    download_parquet_file = "{}/supplier.download.parquet".format(test_path)

    fields = [
        pa.field('s_suppkey', pa.int32()),
        pa.field('s_name', pa.string()),
        pa.field('s_address', pa.string()),
        pa.field('s_nationkey', pa.int32()),
        pa.field('s_phone', pa.string()),
        pa.field('s_acctbal', pa.float32()),
        pa.field('s_comment', pa.string())
    ]

    schema = pa.schema(fields)

    convert_csv_file_to_typed_parquet_file(csv_file='{}/tests/parquet/supplier.csv'.format(ROOT_DIR),
                                           parquet_file=upload_parquet_file,
                                           compression=None,
                                           schema=schema,
                                           ntimes=num_writes)

    cfg = Config(region_name="us-east-1", parameter_validation=False, max_pool_connections=10)
    session = Session()
    s3 = session.client('s3', config=cfg)

    s3.upload_file(upload_parquet_file, 's3filter', 'parquet/supplier.large.typed.parquet')

    s3.download_file('s3filter', 'parquet/supplier.large.typed.parquet', download_parquet_file)

    table = pq.read_table(download_parquet_file)
    df = arrow_to_pandas(table)

    assert len(df) == 10000 * num_writes

    rows = df[df['s_suppkey'] == '1']

    assert len(rows) == num_writes

    row = rows.iloc[0]

    assert row['s_suppkey'] == "1"


def test_write_large_typed_compressed_parquet_file_to_s3():
    num_writes = 500

    test_path = tests_path + "/{}".format(test_write_large_typed_compressed_parquet_file_to_s3.__name__)
    ensure_dir(test_path)

    upload_parquet_file = "{}/supplier.compressed.upload.parquet".format(test_path)
    download_parquet_file = "{}/supplier.compressed.download.parquet".format(test_path)

    fields = [
        pa.field('s_suppkey', pa.int32()),
        pa.field('s_name', pa.string()),
        pa.field('s_address', pa.string()),
        pa.field('s_nationkey', pa.int32()),
        pa.field('s_phone', pa.string()),
        pa.field('s_acctbal', pa.float32()),
        pa.field('s_comment', pa.string())
    ]

    schema = pa.schema(fields)

    convert_csv_file_to_typed_parquet_file(csv_file='{}/tests/parquet/supplier.csv'.format(ROOT_DIR),
                                           parquet_file=upload_parquet_file,
                                           compression='snappy',
                                           schema=schema,
                                           ntimes=num_writes)

    cfg = Config(region_name="us-east-1", parameter_validation=False, max_pool_connections=10)
    session = Session()
    s3 = session.client('s3', config=cfg)

    s3.upload_file(upload_parquet_file, 's3filter', 'parquet/supplier.large.typed.snappy.parquet')

    s3.download_file('s3filter', 'parquet/supplier.large.typed.snappy.parquet', download_parquet_file)

    table = pq.read_table(download_parquet_file)
    df = arrow_to_pandas(table)

    assert len(df) == 10000 * num_writes

    rows = df[df['s_suppkey'] == '1']

    assert len(rows) == num_writes

    row = rows.iloc[0]

    assert row['s_suppkey'] == "1"


def test_write_large_csv_file():
    num_writes = 500

    test_path = tests_path + "/{}".format(test_write_large_csv_file.__name__)
    ensure_dir(test_path)

    upload_csv_file = "{}/supplier.upload.csv".format(test_path)

    df = pd.read_csv('{}/tests/parquet/supplier.csv'.format(ROOT_DIR), sep='|', dtype=str)

    df.to_csv(upload_csv_file, mode='w', sep='|', index=False, header=True)
    for i in range(0, num_writes - 1):
        df.to_csv(upload_csv_file, mode='a', sep='|', index=False, header=False)

    df = pd.read_csv(upload_csv_file, sep='|', header=0, dtype=str)

    assert len(df) == 10000 * num_writes

    rows = df[df['s_suppkey'] == '1']

    assert len(rows) == num_writes

    row = rows.iloc[0]

    assert row['s_suppkey'] == "1"


def test_write_csv_file_to_s3():
    test_path = tests_path + "/{}".format(test_write_csv_file_to_s3.__name__)
    ensure_dir(test_path)

    upload_csv_file = "{}/supplier.upload.csv".format(test_path)
    download_csv_file = "{}/supplier.download.csv".format(test_path)

    df = pd.read_csv('{}/tests/parquet/supplier.csv'.format(ROOT_DIR), sep='|', dtype=str)

    df.to_csv(upload_csv_file, mode='w', sep='|', index=False, header=True)

    cfg = Config(region_name="us-east-1", parameter_validation=False, max_pool_connections=10)
    session = Session()
    s3 = session.client('s3', config=cfg)

    s3.upload_file(upload_csv_file, 's3filter', 'parquet/supplier.csv')

    s3.download_file('s3filter', 'parquet/supplier.csv', download_csv_file)

    df = pd.read_csv(download_csv_file, sep='|', header=0, dtype=str)

    assert len(df) == 10000

    rows = df[df['s_suppkey'] == '1']

    assert len(rows) == 1

    row = rows.iloc[0]

    assert row['s_suppkey'] == "1"


def test_write_large_csv_file_to_s3():
    num_writes = 500

    test_path = tests_path + "/{}".format(test_write_large_csv_file_to_s3.__name__)
    ensure_dir(test_path)

    upload_csv_file = "{}/supplier.upload.csv".format(test_path)
    download_csv_file = "{}/supplier.download.csv".format(test_path)

    df = pd.read_csv('{}/tests/parquet/supplier.csv'.format(ROOT_DIR), sep='|', dtype=str)

    df.to_csv(upload_csv_file, mode='w', sep='|', index=False, header=True)
    for i in range(0, num_writes - 1):
        df.to_csv(upload_csv_file, mode='a', sep='|', index=False, header=False)

    cfg = Config(region_name="us-east-1", parameter_validation=False, max_pool_connections=10)
    session = Session()
    s3 = session.client('s3', config=cfg)

    s3.upload_file(upload_csv_file, 's3filter', 'parquet/supplier.large.csv')

    s3.download_file('s3filter', 'parquet/supplier.large.csv', download_csv_file)

    df = pd.read_csv(download_csv_file, sep='|', header=0, dtype=str)

    assert len(df) == 10000 * num_writes

    rows = df[df['s_suppkey'] == '1']

    assert len(rows) == num_writes

    row = rows.iloc[0]

    assert row['s_suppkey'] == "1"


def test_s3_select_from_csv():
    cfg = Config(region_name="us-east-1", parameter_validation=False, max_pool_connections=10)
    session = Session()
    s3 = session.client('s3', config=cfg)

    response = s3.select_object_content(Bucket='s3filter', Key='parquet/supplier.csv',
                                        Expression='select sum(cast(s_acctbal as float)) from s3Object',
                                        ExpressionType='SQL',
                                        InputSerialization={
                                            'CompressionType': 'NONE',
                                            'CSV': {'FileHeaderInfo': 'Use', 'RecordDelimiter': '\n',
                                                    'FieldDelimiter': '|'}
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

    assert len(df) == 1

    assert pd.to_numeric(df.iloc[0]['_0']) == pytest.approx(45103548.64999)


def test_write_small_parquet_files():
    test_path = tests_path + "/{}".format(test_write_small_parquet_files.__name__)
    ensure_dir(test_path)

    schema = pa.schema([pa.field('c', pa.int32(), False)])

    for i in range(0, 10000):
        vals = [x for x in range(0, i + 1)]

        parquet_file = "{}/small.{}.parquet".format(test_path, i)

        df = pd.DataFrame({'c': vals})
        table = pa.Table.from_pandas(df, schema=schema, preserve_index=False)
        pq.write_table(table, parquet_file, version='2.0', compression='none', use_dictionary=False,
                       flavor='spark')


def test_write_small_parquet_file_to_s3():
    test_path = tests_path + "/{}".format(test_write_small_parquet_file_to_s3.__name__)
    test_path2 = tests_path + "/{}".format(test_write_small_parquet_files.__name__)
    ensure_dir(test_path)

    upload_parquet_file = "{}/small.9999.parquet".format(test_path2)
    download_parquet_file = "{}/small.9999.download.parquet".format(test_path)

    table1 = pq.read_table(upload_parquet_file)

    cfg = Config(region_name="us-east-1", parameter_validation=False, max_pool_connections=10)
    session = Session()
    s3 = session.client('s3', config=cfg)

    s3.upload_file(upload_parquet_file, 's3filter', 'parquet/small.9999.parquet')

    s3.download_file('s3filter', 'parquet/small.9999.parquet', download_parquet_file)

    table = pq.read_table(download_parquet_file)
    df = arrow_to_pandas(table)

    assert len(df) == 10000


def test_write_small_multi_column_parquet_files():
    test_path = tests_path + "/{}".format(test_write_small_multi_column_parquet_files.__name__)
    ensure_dir(test_path)

    schema = pa.schema([pa.field('a', pa.int32(), False), pa.field('b', pa.int32(), False)])

    for i in range(0, 10000):
        vals = [x for x in range(0, i + 1)]

        parquet_file = "{}/small.multicolumn.{}.parquet".format(test_path, i)

        df = pd.DataFrame({'a': vals, 'b': vals})
        table = pa.Table.from_pandas(df, schema=schema, preserve_index=False)
        pq.write_table(table, parquet_file, version='2.0', compression='none', use_dictionary=False,
                       flavor='spark')


def test_write_small_multi_column_parquet_files_to_s3():
    test_path = tests_path + "/{}".format(test_write_small_multi_column_parquet_files_to_s3.__name__)
    test_path2 = tests_path + "/{}".format(test_write_small_multi_column_parquet_files.__name__)
    ensure_dir(test_path)

    upload_parquet_file = "{}/small.multicolumn.9999.parquet".format(test_path2)
    download_parquet_file = "{}/small.multicolumn.9999.download.parquet".format(test_path)

    table1 = pq.read_table(upload_parquet_file)

    cfg = Config(region_name="us-east-1", parameter_validation=False, max_pool_connections=10)
    session = Session()
    s3 = session.client('s3', config=cfg)

    s3.upload_file(upload_parquet_file, 's3filter', 'parquet/small.multicolumn.9999.parquet')

    s3.download_file('s3filter', 'parquet/small.multicolumn.9999.parquet', download_parquet_file)

    table = pq.read_table(download_parquet_file)
    df = arrow_to_pandas(table)

    assert len(df) == 10000


def test_s3_select_from_small_parquet():
    cfg = Config(region_name="us-east-1", parameter_validation=False, max_pool_connections=10)
    session = Session()
    s3 = session.client('s3', config=cfg)

    response = s3.select_object_content(Bucket='s3filter', Key='parquet/small.9999.parquet',
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

    assert len(df) == 10000

    print()
    print("{} | {}".format(test_s3_select_from_small_parquet.__name__, cursor.bytes_scanned))
    print("{} | {}".format(test_s3_select_from_small_parquet.__name__, cursor.bytes_processed))
    print("{} | {}".format(test_s3_select_from_small_parquet.__name__, cursor.bytes_returned))


def test_s3_select_all_from_small_multicolumn_parquet():
    cfg = Config(region_name="us-east-1", parameter_validation=False, max_pool_connections=10)
    session = Session()
    s3 = session.client('s3', config=cfg)

    response = s3.select_object_content(Bucket='s3filter', Key='parquet/small.multicolumn.9999.parquet',
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

    assert len(df) == 10000

    print()
    print("{} | {}".format(test_s3_select_from_small_parquet.__name__, cursor.bytes_scanned))
    print("{} | {}".format(test_s3_select_from_small_parquet.__name__, cursor.bytes_processed))
    print("{} | {}".format(test_s3_select_from_small_parquet.__name__, cursor.bytes_returned))


def test_s3_select_a_from_small_multicolumn_parquet():
    cfg = Config(region_name="us-east-1", parameter_validation=False, max_pool_connections=10)
    session = Session()
    s3 = session.client('s3', config=cfg)

    response = s3.select_object_content(Bucket='s3filter', Key='parquet/small.multicolumn.9999.parquet',
                                        Expression='select a from s3Object', ExpressionType='SQL',
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

    assert len(df) == 10000

    print()
    print("{} | {}".format(test_s3_select_from_small_parquet.__name__, cursor.bytes_scanned))
    print("{} | {}".format(test_s3_select_from_small_parquet.__name__, cursor.bytes_processed))
    print("{} | {}".format(test_s3_select_from_small_parquet.__name__, cursor.bytes_returned))


def test_s3_select_b_from_small_multicolumn_parquet():
    cfg = Config(region_name="us-east-1", parameter_validation=False, max_pool_connections=10)
    session = Session()
    s3 = session.client('s3', config=cfg)

    response = s3.select_object_content(Bucket='s3filter', Key='parquet/small.multicolumn.9999.parquet',
                                        Expression='select b from s3Object', ExpressionType='SQL',
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

    assert len(df) == 10000

    print()
    print("{} | {}".format(test_s3_select_from_small_parquet.__name__, cursor.bytes_scanned))
    print("{} | {}".format(test_s3_select_from_small_parquet.__name__, cursor.bytes_processed))
    print("{} | {}".format(test_s3_select_from_small_parquet.__name__, cursor.bytes_returned))


def test_s3_select_b_filtered_from_small_multicolumn_parquet():
    cfg = Config(region_name="us-east-1", parameter_validation=False, max_pool_connections=10)
    session = Session()
    s3 = session.client('s3', config=cfg)

    response = s3.select_object_content(Bucket='s3filter', Key='parquet/small.multicolumn.9999.parquet',
                                        Expression='select a from s3Object where a < 5000', ExpressionType='SQL',
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

    assert len(df) == 5000

    print()
    print("{} | {}".format(test_s3_select_from_small_parquet.__name__, cursor.bytes_scanned))
    print("{} | {}".format(test_s3_select_from_small_parquet.__name__, cursor.bytes_processed))
    print("{} | {}".format(test_s3_select_from_small_parquet.__name__, cursor.bytes_returned))


def test_write_small_encodable_multi_column_parquet_files():
    test_path = tests_path + "/{}".format(test_write_small_encodable_multi_column_parquet_files.__name__)
    ensure_dir(test_path)

    schema = pa.schema([pa.field('a', pa.float64(), nullable=False), pa.field('b', pa.float64(), nullable=False)])

    for i in range(0, 10000):
        vals = [0 for x in range(0, i + 1)]

        parquet_file = "{}/small.encodable.multicolumn.{}.parquet".format(test_path, i)

        df = pd.DataFrame({'a': vals, 'b': vals})
        table = pa.Table.from_pandas(df, schema=schema)
        pq.write_table(table, parquet_file, compression=None, use_dictionary=True)


def test_select_all_with_cursor():
    cfg = Config(region_name="us-east-1", parameter_validation=False, max_pool_connections=10,
                 s3={'payload_signing_enabled': False})
    session = Session()
    s3 = session.client('s3', use_ssl=False, verify=False, config=cfg)

    num_rows = 0

    cur = PandasCursor(s3) \
        .parquet() \
        .select('parquet/small.multicolumn.9999.parquet', 'select * from s3Object')

    try:
        dfs = cur.execute()
        for df in dfs:
            num_rows += len(df)
            print("{}:{}".format(num_rows, df))

        assert num_rows == 10000
    finally:
        cur.close()


def test_select_projected_filtered_topk_with_cursor():
    cfg = Config(region_name="us-east-1", parameter_validation=False, max_pool_connections=10,
                 s3={'payload_signing_enabled': False})
    session = Session()
    s3 = session.client('s3', use_ssl=False, verify=False, config=cfg)

    num_rows = 0

    cur = PandasCursor(s3) \
        .parquet() \
        .select('parquet/small.multicolumn.9999.parquet', 'select b from s3Object where a > 5000 limit 100')

    try:
        dfs = cur.execute()
        for df in dfs:
            num_rows += len(df)
            print("{}:{}".format(num_rows, df))

        assert num_rows == 100
    finally:
        cur.close()
