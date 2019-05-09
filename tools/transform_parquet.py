import multiprocessing

import boto3
import matplotlib
from pyspark.sql import SparkSession

matplotlib.use('Agg')


def main():
    from_path_base = "s3a://s3filter/parquet/tpch-sf10/lineitem_sharded1RG/lineitem.typed.1RowGroup.parquet"
    # temp_path_base_header = "s3filter/parquet/tpch-sf10/lineitem_sharded_rg-256m/_temporary/lineitem"
    temp_path_base_path = "parquet/tpch-sf10/lineitem_sharded_rg-256m/lineitem"
    temp_path_base_header = "s3filter/%s" % temp_path_base_path
    temp_path_base_footer = "parquet"

    ACCESS_KEY = "**********"
    SECRET_KEY = "**********"

    spark = SparkSession.builder.master("local[*]") \
        .appName("s3filter") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.7") \
        .config("spark.hadoop.fs.s3a.access.key", ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.secret.key", SECRET_KEY) \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain") \
        .getOrCreate()

    for i in range(96, 97):

        from_path = "{}.{}".format(from_path_base, i)
        # temp_shard_path = "{}.{}.{}".format(temp_path_base_header, i, temp_path_base_footer)
        temp_shard_base_path = "{}.{}.{}".format(temp_path_base_path, temp_path_base_footer, i)
        temp_shard_path = "{}.{}.{}".format(temp_path_base_header, temp_path_base_footer, i)
        temp_shard_path_with_scheme = "s3a://{}".format(temp_shard_path)
        to_path = "{}.{}.{}".format(temp_path_base_path, i, temp_path_base_footer)

        print("Processing {}: {} -> {} -> {}".format(i, from_path, temp_shard_path_with_scheme, to_path))

        # Read and write parquet

        print("  Generating {} -> {}".format(from_path, temp_shard_path_with_scheme))
        df = spark.read.parquet(from_path)
        df = df.repartition(multiprocessing.cpu_count())
        # df.show()
        # df = df.orderBy("l_orderkey")
        df = df.coalesce(1)
        df.write \
            .option("parquet.block.size", str(128 * 1024 * 1024)) \
            .mode("overwrite") \
            .parquet(temp_shard_path_with_scheme, compression="none")

        # Get files written by spark, should be only one, and move them to a well known destination
        s3 = boto3.resource('s3')
        bucket = s3.Bucket("s3filter")
        for object in bucket.objects.filter(Prefix=temp_shard_base_path):
            if "/".join(object.key.split("/")[:-1]) == temp_shard_base_path and object.key.endswith("parquet"):
                print("  Copying {} -> {}".format(object.key, to_path))
                copy_source = {
                    'Bucket': "s3filter",
                    'Key': object.key
                }
                bucket.copy(copy_source, to_path)

                # Delete the object source dir
                # source_object = s3.Object('s3filter', temp_shard_path_with_scheme)
                # source_object.delete()


    spark.stop()


if __name__ == "__main__":
    main()
