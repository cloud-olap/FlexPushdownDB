#!/usr/bin/env python
# hash partition datasets on the given hash keys into multiple storage nodes
# this will upload all partitions to S3 and generate the corresponding object map

import os
import json

# fixed parameters
bucket = "flexpushdowndb"

# configurable parameters
num_rows_per_part = 20000
num_nodes = 1
schema = "tpch-sf0.01/parquet/"
new_schema = "tpch-sf0.01-{}-node-hash-part/parquet/".format(num_nodes)
hash_keys = {
    "lineitem": "l_orderkey",
    "orders": "o_orderkey",
    "customer": "c_nationkey",
    "part": "p_partkey",
    "partsupp": "ps_partkey",
    "supplier": "s_nationkey",
    "nation": "n_regionkey",
    "region": "r_regionkey"
}
build_dir_name = "cmake-build-debug"

# other parameters
this_file_dir = os.path.dirname(os.path.realpath(__file__))
resource_path = os.path.dirname(os.path.dirname(this_file_dir))
schema_path = os.path.join(os.path.join(resource_path, "metadata"), schema)
part_exec_dir = os.path.join(os.path.join(os.path.dirname(resource_path), build_dir_name), "fpdb-executor/")

# read "schema.json"
partition_map = {}
format_ = ""
with open(os.path.join(schema_path, "schema.json"), 'r') as f:
    schema_data = json.load(f)
    for table in schema_data["tables"]:
        partition_map[table["name"]] = table["numPartitions"]
        if format_ == "":
            format_ = table["format"]["name"]

# go to exec dir
my_cwd = os.getcwd()
os.chdir(part_exec_dir)

# run partition exec for each table
try:
    is_first_table = True
    for table in partition_map:
        # params
        print("Partition table: " + table + "... ", end='', flush=True)
        params = "{} {} {} {} {} {} {}".format(schema,
                                               new_schema,
                                               table,
                                               partition_map[table],
                                               num_nodes,
                                               num_rows_per_part,
                                               is_first_table)
        is_first_table = False
        if table in hash_keys:
            params = "{} {}".format(params, hash_keys[table])
        # run exec
        os.system("./fpdb-executor-hash-partitioner-exec {}".format(params))
        print("done")
except Exception as e:
    print("Error: {}".format(e))
print("Tables partitioned.")

# go back
os.chdir(my_cwd)
