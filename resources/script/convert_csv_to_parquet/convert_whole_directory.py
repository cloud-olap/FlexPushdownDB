# Build 'fpdb-tuple-converter-exec' before running this
# Example usage: 'python convert_whole_directory.py
# ~/Desktop/FlexPushdownDB-Dev/fpdb-main/test-resources/fpdb-store-0/flexpushdowndb/tpch-sf0.01/csv
# parquet tpch/schema_format ~/Desktop/FlexPushdownDB-Dev/cmake-build-debug'

import sys
import os
from multiprocessing import Process


def run_command(cmd):
    os.system(cmd)


if len(sys.argv) < 5:
    raise Exception("Please specify csv folder path, parquet folder path, schema format path, and fpdb build path")
csv_path = sys.argv[1]
parquet_path = sys.argv[2]
schema_format_path = sys.argv[3]
fpdb_build_path = sys.argv[4]

partition_folder_prefix = '_sharded'
csv_file_suffix = '.tbl'
parquet_file_suffix = '.parquet'
schema_format_file_suffix = '.json'
converter_exec_path = fpdb_build_path + "/fpdb-tuple/fpdb-tuple-converter-exec"
root_files = os.listdir(csv_path)
tables = [schema_format_file[:-len(schema_format_file_suffix)] for schema_format_file in os.listdir(schema_format_path)]

os.system("rm -rf " + parquet_path)
os.system("mkdir -p " + parquet_path)

procs = []

for table in tables:
    schema_format_file_path = "{}/{}{}".format(schema_format_path, table, schema_format_file_suffix)
    csv_file_name = table + csv_file_suffix

    if csv_file_name in root_files:
        csv_file_path = "{}/{}".format(csv_path, csv_file_name)
        parquet_file_path = "{}/{}{}".format(parquet_path, table, parquet_file_suffix)
        convert_cmd = "{} {} {} {} {}".format(converter_exec_path,
                                              csv_file_path,
                                              parquet_file_path,
                                              'uncompressed',
                                              schema_format_file_path)
        if __name__ == '__main__':
            p = Process(target=run_command, args=(convert_cmd,))
            procs.append(p)
            p.start()

    else:
        csv_partitions_folder = "{}/{}{}".format(csv_path, table, partition_folder_prefix)
        parquet_partitions_folder = "{}/{}{}".format(parquet_path, table, partition_folder_prefix)
        csv_partitions_file_name = os.listdir(csv_partitions_folder)
        for csv_partition_file_name in csv_partitions_file_name:
            partition_id = csv_partition_file_name[csv_partition_file_name.rfind('.')+1:]
            parquet_partition_file_name = "{}.{}.{}".format(table, 'parquet', partition_id)
            csv_partition_file_path = '{}/{}'.format(csv_partitions_folder, csv_partition_file_name)
            parquet_partition_file_path = '{}/{}'.format(parquet_partitions_folder, parquet_partition_file_name)
            convert_cmd = "{} {} {} {} {}".format(converter_exec_path,
                                                  csv_partition_file_path,
                                                  parquet_partition_file_path,
                                                  'uncompressed',
                                                  schema_format_file_path)
            if __name__ == '__main__':
                p = Process(target=run_command, args=(convert_cmd,))
                procs.append(p)
                p.start()

for p in procs:
    p.join()
