import os

import pandas as pd
import pytest
from boto3 import Session
from botocore.config import Config

from s3filter import ROOT_DIR
from s3filter.sql.pandas_cursor import PandasCursor

tests_path = os.path.abspath("{}/../build/tests".format(ROOT_DIR))


def test_s3_select_from_large_csv(benchmark):
    def run():
        cfg = Config(region_name="us-east-1", parameter_validation=False, max_pool_connections=10)
        session = Session()
        s3 = session.client('s3', config=cfg)

        response = s3.select_object_content(Bucket='s3filter', Key='parquet/supplier.large.csv',
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

        assert pd.to_numeric(df.iloc[0]['_0']) == pytest.approx(22551774325.00404)

        print("{} | {}".format(test_s3_select_from_large_csv.__name__, cursor.bytes_scanned))
        print("{} | {}".format(test_s3_select_from_large_csv.__name__, cursor.bytes_processed))
        print("{} | {}".format(test_s3_select_from_large_csv.__name__, cursor.bytes_returned))

    benchmark(run)


def test_s3_select_from_large_parquet(benchmark):
    def run():
        cfg = Config(region_name="us-east-1", parameter_validation=False, max_pool_connections=10)
        session = Session()
        s3 = session.client('s3', config=cfg)
        response = s3.select_object_content(Bucket='s3filter', Key='parquet/supplier.large.parquet',
                                            Expression='select sum(cast(s_acctbal as float)) from s3Object',
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
        assert len(df) == 1
        assert pd.to_numeric(df.iloc[0]['_0']) == pytest.approx(22551774325.00404)

        print("{} | {}".format(test_s3_select_from_large_parquet.__name__, cursor.bytes_scanned))
        print("{} | {}".format(test_s3_select_from_large_parquet.__name__, cursor.bytes_processed))
        print("{} | {}".format(test_s3_select_from_large_parquet.__name__, cursor.bytes_returned))

    benchmark(run)


def test_s3_select_from_large_typed_parquet(benchmark):
    def run():
        cfg = Config(region_name="us-east-1", parameter_validation=False, max_pool_connections=10)
        session = Session()
        s3 = session.client('s3', config=cfg)
        response = s3.select_object_content(Bucket='s3filter', Key='parquet/supplier.large.typed.parquet',
                                            Expression='select sum(s_acctbal) from s3Object',
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
        assert len(df) == 1
        assert pd.to_numeric(df.iloc[0]['_0']) == pytest.approx(22551774325.00404)

        print("{} | {}".format(test_s3_select_from_large_typed_parquet.__name__, cursor.bytes_scanned))
        print("{} | {}".format(test_s3_select_from_large_typed_parquet.__name__, cursor.bytes_processed))
        print("{} | {}".format(test_s3_select_from_large_typed_parquet.__name__, cursor.bytes_returned))

    benchmark(run)


def test_s3_select_from_large_typed_compressed_parquet(benchmark):
    def run():
        cfg = Config(region_name="us-east-1", parameter_validation=False, max_pool_connections=10)
        session = Session()
        s3 = session.client('s3', config=cfg)
        response = s3.select_object_content(Bucket='s3filter', Key='parquet/supplier.large.typed.snappy.parquet',
                                            Expression='select sum(s_acctbal) from s3Object',
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
        assert len(df) == 1
        assert pd.to_numeric(df.iloc[0]['_0']) == pytest.approx(22551774325.00404)

        print("{} | {}".format(test_s3_select_from_large_typed_compressed_parquet.__name__, cursor.bytes_scanned))
        print("{} | {}".format(test_s3_select_from_large_typed_compressed_parquet.__name__, cursor.bytes_processed))
        print("{} | {}".format(test_s3_select_from_large_typed_compressed_parquet.__name__, cursor.bytes_returned))

    benchmark(run)
