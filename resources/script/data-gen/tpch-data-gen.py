# Put this under tpch-dbgen/
# Make sure dbgen has been built, to build it, do as follows:
#   1) git clone https://github.com/electrum/tpch-dbgen.git
#   2) run 'make' inside the repo

import os
import platform
import math
from multiprocessing import Process

# configurable parameters
sf = 0.01
num_partitions_dict = {
    'nation': 1,
    'region': 1,
    'part': 1,
    'supplier': 1,
    'partsupp': 1,
    'customer': 1,
    'orders': 1,
    'lineitem': 1
}


def get_num_lines_partition(num_lines, num_partitions):
    return int(math.ceil(float(num_lines) / float(num_partitions)))


def get_num_digits_suffix(num_partitions):
    return int(math.ceil(math.log(num_partitions, 10)))


def run_command(cmd):
    os.system(cmd)


def format_data_for_table(table, column_names, num_partitions):
    global split_func, data_dir

    table_file = table + ".tbl"
    partition_file_prefix = table_file + "."
    partition_dir = table + "_sharded"

    # split table file if it has multiple partitions
    if num_partitions > 1:
        # get num of lines
        num_lines = int(os.popen('wc -l < {}'.format(table_file)).read())

        # split table into partitions
        num_lines_partition = get_num_lines_partition(num_lines, num_partitions)
        num_digits_suffix = get_num_digits_suffix(num_partitions)
        os.system('{} -a {} -d -l {} {} {}'.format(split_func,
                                                   num_digits_suffix,
                                                   num_lines_partition,
                                                   table_file,
                                                   partition_file_prefix))

        # remove leading 0s in the suffix of partitions
        partition_files = []
        for i in range(num_partitions):
            old_name = partition_file_prefix + str(i).zfill(num_digits_suffix)
            new_name = partition_file_prefix + str(i)
            os.system('mv {} {} 2>/dev/null'.format(old_name, new_name))
            partition_files.append(new_name)

        # remove '|' at the end of each row
        procs = []
        for partition_file in partition_files:
            if platform.system() == "Darwin":
                cmd_remove_end = 'sed -i \'\' \'s/.$//\' {}'.format(partition_file)
            else:
                cmd_remove_end = 'sed -i \'s/.$//\' {}'.format(partition_file)
            p = Process(target=run_command, args=(cmd_remove_end,))
            procs.append(p)
            p.start()
        for p in procs:
            p.join()

        # add column names
        procs = []
        for partition_file in partition_files:
            if platform.system() == "Darwin":
                cmd_add_column_names = 'sed -i \'\' \'1s/^/{}\\\'$\'\\n/\' {}'.format(column_names, partition_file)
            else:
                cmd_add_column_names = 'sed -i \'1i {}\' {}'.format(column_names, partition_file)
            p = Process(target=run_command, args=(cmd_add_column_names,))
            procs.append(p)
            p.start()
        for p in procs:
            p.join()

        # move partitions into the directory
        os.system('mkdir {}'.format(partition_dir))
        os.system('mv {}* {}/'.format(partition_file_prefix, partition_dir))

        # move data into data_dir
        os.system('mv {} {}'.format(partition_dir, data_dir))

    else:
        # remove '|' at the end of each row
        if platform.system() == "Darwin":
            os.system('sed -i \'\' \'s/.$//\' {}'.format(table_file))
        else:
            os.system('sed -i \'s/.$//\' {}'.format(table_file))

        # add column names
        if platform.system() == "Darwin":
            os.system('sed -i \'\' \'1s/^/{}\\\'$\'\\n/\' {}'.format(column_names, table_file))
        else:
            os.system('sed -i \'1i {}\' {}'.format(column_names, table_file))

        # move data into data_dir
        os.system('mv {} {}'.format(table_file, data_dir))


# fixed parameters
tables = ['nation', 'region', 'part', 'supplier', 'partsupp', 'customer', 'orders', 'lineitem']
column_names_dict = {
    'nation': 'N_NATIONKEY|N_NAME|N_REGIONKEY|N_COMMENT',
    'region': 'R_REGIONKEY|R_NAME|R_COMMENT',
    'part': 'P_PARTKEY|P_NAME|P_MFGR|P_BRAND|P_TYPE|P_SIZE|P_CONTAINER|P_RETAILPRICE|P_COMMENT',
    'supplier': 'S_SUPPKEY|S_NAME|S_ADDRESS|S_NATIONKEY|S_PHONE|S_ACCTBAL|S_COMMENT',
    'partsupp': 'PS_PARTKEY|PS_SUPPKEY|PS_AVAILQTY|PS_SUPPLYCOST|PS_COMMENT',
    'customer': 'C_CUSTKEY|C_NAME|C_ADDRESS|C_NATIONKEY|C_PHONE|C_ACCTBAL|C_MKTSEGMENT|C_COMMENT',
    'orders': 'O_ORDERKEY|O_CUSTKEY|O_ORDERSTATUS|O_TOTALPRICE|O_ORDERDATE|O_ORDERPRIORITY|O_CLERK|O_SHIPPRIORITY'
              '|O_COMMENT',
    'lineitem': 'L_ORDERKEY|L_PARTKEY|L_SUPPKEY|L_LINENUMBER|L_QUANTITY|L_EXTENDEDPRICE|L_DISCOUNT|L_TAX|L_RETURNFLAG'
                '|L_LINESTATUS|L_SHIPDATE|L_COMMITDATE|L_RECEIPTDATE|L_SHIPINSTRUCT|L_SHIPMODE|L_COMMENT '
}

# pick appropriate split tool
if platform.system() == "Darwin":
    split_func = "gsplit"
else:
    split_func = "split"

# create the directory to put data
data_dir = 'tpch-sf' + str(sf)
os.system('rm -rf {}'.format(data_dir))
os.system('mkdir {}'.format(data_dir))

# generate data
os.system('./dbgen -s {}'.format(sf))
print("Tables generated.")

# format for each table
for table in tables:
    print("Formatting " + table + "... ", end='', flush=True)
    format_data_for_table(table, column_names_dict[table], num_partitions_dict[table])
    print('done')
print("Tables formatted.")

os.system('rm -f *.tbl')
