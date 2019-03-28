import os

#import matplotlib
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import s3fs

#matplotlib.use('Agg')


def main():
    # cfg = Config(region_name="us-east-1", parameter_validation=False, max_pool_connections=10)
    # session = Session()
    # s3 = session.client('s3', config=cfg)

    dryRun = False
    #paths = ["s3://s3filter/tpch-sf1", "s3://s3filter/tpch-sf10", "s3://s3filter/tpch-sf100"]
    paths = ["s3://s3filter/tpch-sf10/lineitem_sharded"]

    fs = s3fs.S3FileSystem()

    for path in paths:
        process_path(path, dryRun, fs)


def process_path(sf_, dryRun, fs, ):

    object_paths = fs.walk(sf_)

    object_count = 0
    num_objects = len(object_paths)
    for src_object_path in object_paths:

        object_count += 1

        extension1, extension2, file_name, object_dir_path, bucket_name, object_file_basename = \
            parse_object_path(src_object_path)
        dest_object_path = generate_dest_object_path(bucket_name, extension2, object_dir_path, object_file_basename)

        if extension1 == "csv" or extension2 == "csv" or extension1 == "tbl" or extension2 == "tbl":
            print(
                "[{}/{} - {}]  Processing {} -> {}".format(object_count, num_objects, src_object_path, src_object_path,
                                                           dest_object_path))
            process_object(src_object_path, dest_object_path, dryRun, fs)
        else:
            print("[{}/{} - {}]  Skipping".format(object_count, num_objects, src_object_path))


def convert_dates(chunk, schema):
    for i in range(0, len(schema)):

        if schema[i].type == pa.date32():
            chunk[chunk.columns[i]] = pd.to_datetime(chunk[chunk.columns[i]])

    return chunk

def convert_lineitem_date_columns_panda(df):
    df['l_shipdate'] = pd.to_datetime(df['l_shipdate'], format='%Y-%m-%d')
    df['l_commitdate'] = pd.to_datetime(df['l_commitdate'], format='%Y-%m-%d')
    df['l_receiptdate'] = pd.to_datetime(df['l_receiptdate'], format='%Y-%m-%d')


def process_object(src_object_path, dest_object_path, dryRun, fs):
    num_lines = 0
    chunk_size = 1000

    writer = None
    #schema = None
    line_item_schema = pa.schema([
      pa.field('l_orderkey', pa.int64(), False),
      pa.field('l_partkey', pa.int64(), False),
      pa.field('l_suppkey', pa.int64(), False),
      pa.field('l_linenumber', pa.int32(), True),
      pa.field('l_quantity', pa.float32(), True),
      pa.field('l_extendedprice', pa.float32(), True),
      pa.field('l_discount', pa.float32(), True),
      pa.field('l_tax', pa.float32(), True),
      pa.field('l_returnflag', pa.string(), True), # originally supposed to be float32
      pa.field('l_linestatus', pa.string(), True),
      pa.field('l_shipdate', pa.date64(), True), #originally supposed to be a string
      pa.field('l_commitdate', pa.date64(), True),#originally supposed to be a date32
      pa.field('l_receiptdate', pa.date64(), True),#originally supposed to be a date32
      pa.field('l_shipinstruct', pa.string(), True),
      pa.field('l_shipmode', pa.string(), True),
      pa.field('l_comment', pa.string(), True)
    ])

    chunks = pd.read_csv("s3://{}".format(src_object_path), sep="|", header=0, chunksize=chunk_size, engine="c")
    for chunk in chunks:

        num_lines += len(chunk)
        print("[{}]  Read chunk: num_lines: {} ".format(src_object_path, num_lines))

        if num_lines > 0:
            chunk = chunk.drop(chunk.columns[len(chunk.columns) - 1], axis=1)
            convert_lineitem_date_columns_panda(chunk)
            #if schema is None:
            #    schema = infer_schema(chunk)

            #chunk = convert_dates(chunk, schema)

            table = pa.Table.from_pandas(df=chunk, schema=line_item_schema, preserve_index=False)

            if not dryRun:
                if writer is None:
                    writer = pq.ParquetWriter(where="s3://" + dest_object_path,
                                              schema=table.schema,
                                              use_dictionary=False,
                                              compression='NONE',
                                              filesystem=fs)

                writer.write_table(table)
                print("[{}]  Wrote chunk: num_lines: {} ".format(src_object_path, num_lines))
        else:
            print("[{}]  Skipping, object is empty: num_lines: {} ".format(src_object_path, num_lines))

    if not dryRun and writer is not None:
        writer.close()


def infer_schema(chunk):
    fields = []

    columns = chunk.columns
    dtypes = chunk.dtypes

    i = 0
    for column in columns:

        dtype = dtypes[i]

        if dtype.name == 'object':
            if column.endswith('date'):
                fields.append(pa.field(column, pa.date32()))
            else:
                fields.append(pa.field(column, pa.string()))
        else:
            fields.append(pa.field(column, pa.type_for_alias(dtype.name)))

        i += 1

    schema = pa.schema(fields)

    return schema


def generate_dest_object_path(bucket_name, extension2, object_dir_path, object_file_basename):
    dest_object_dir_path = bucket_name + "/parquet/" + object_dir_path + "1RG"

    dest_object_name = object_file_basename + ".typed.1RowGroup.parquet"
    if extension2 != "":
        dest_object_name = dest_object_name + "." + extension2

    dest_object_path = dest_object_dir_path + "/" + dest_object_name

    return dest_object_path


def parse_object_path(file_path):
    bucket_name = file_path.split("/")[0]
    object_name = os.path.basename(file_path)
    object_dir_path = "/".join(file_path.split("/")[1:-1])

    split_file_name = object_name.split(".")
    if len(split_file_name) == 2:
        object_file_basename = split_file_name[:-1][0]
        extension1 = split_file_name[-1:][0]
        extension2 = ""
    elif len(split_file_name) >= 3:
        object_file_basename = ".".join(split_file_name[:-2])
        extension1 = split_file_name[-2:-1][0]
        extension2 = split_file_name[-1:][0]
    else:
        object_file_basename = object_name
        extension1 = ""
        extension2 = ""
    return extension1, extension2, object_name, object_dir_path, bucket_name, object_file_basename


if __name__ == "__main__":
    main()
