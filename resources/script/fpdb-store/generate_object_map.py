import os
import json

# configurable parameters
schema = "tpch-sf10/csv/"
type_ = "round_robin"
object_map_file_name = "fpdb_store_object_map.json"

# other parameters
resource_path = os.path.dirname(os.path.dirname(os.getcwd()))
schema_path = os.path.join(os.path.join(resource_path, "metadata"), schema)


def generate_object_map():
    if type_ == "round_robin":
        generate_object_map_round_robin()
    else:
        raise Exception("Unknown type of generating object map:", type_)


def generate_object_map_round_robin():
    # num nodes
    num_nodes = get_num_nodes()

    # read "schema.json"
    partition_map = {}
    format_ = ""
    with open(os.path.join(schema_path, "schema.json"), 'r') as f:
        schema_data = json.load(f)
        for table in schema_data["tables"]:
            partition_map[table["name"]] = table["numPartitions"]
            if format_ == "":
                format_ = table["format"]["name"]
    if format_ == "csv":
        format_ = "tbl"

    # make object map
    object_map = []
    single_partition_table_id = 0
    for table in partition_map:
        num_partitions = partition_map[table]
        if num_partitions == 1:
            object_entry = {
                "object": table + "." + format_,
                "nodeId": single_partition_table_id % num_nodes
            }
            object_map.append(object_entry)
            single_partition_table_id += 1
        else:
            for i in range(num_partitions):
                object_entry = {
                    "object": table + "_sharded/" + table + "." + format_ + "." + str(i),
                    "nodeId": i % num_nodes
                }
                object_map.append(object_entry)

    # write object map to json file
    dict_to_file = {
        "schemaName": schema,
        "numNodes": num_nodes,
        "objectMap": object_map
    }
    object_map_path = os.path.join(schema_path, object_map_file_name)
    with open(object_map_path, 'w') as json_file:
        json.dump(dict_to_file, json_file, indent=2)


def get_num_nodes():
    fpdb_store_ips_path = os.path.join(resource_path, "config/fpdb-store_ips")
    return sum(1 for line in open(fpdb_store_ips_path))


generate_object_map()
