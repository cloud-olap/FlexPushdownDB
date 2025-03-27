# Put this under tpch-dbgen/
# Make sure dbgen has been built, to build it, do as follows:
#   1) git clone https://github.com/electrum/tpch-dbgen.git
#   2) run 'make' inside the repo

import os
import platform
import math
import multiprocessing
from multiprocessing import Pool

# configurable parameters
sf = 1000
num_partitions_dict = {
    'nation': 1,
    'region': 1,
    'part': 100,
    'supplier': 16,
    'partsupp': 500,
    'customer': 150,
    'orders': 1000,
    'lineitem': 2000
}
direct_gen_partitions = True    # whether to use built-in funcs to directly generate partitions instead of
                                # generating the whole then splitting into partitions

# const parameters
table_to_flag = {
    'nation': 'n',
    'region': 'r',
    'part': 'P',
    'supplier': 's',
    'partsupp': 'S',
    'customer': 'c',
    'orders': 'O',
    'lineitem': 'L'
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
        if not direct_gen_partitions:
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

        # rename files
        os.system('chmod -R +rw .')
        partition_files = []
        for i in range(num_partitions):
            if direct_gen_partitions:
                # rename file since start index from built-in gen is *.1 but we need *.0
                old_name = partition_file_prefix + str(i + 1)
            else:
                # remove leading 0s in the suffix of partitions
                old_name = partition_file_prefix + str(i).zfill(num_digits_suffix)
            new_name = partition_file_prefix + str(i)
            os.system('mv {} {} 2>/dev/null'.format(old_name, new_name))
            partition_files.append(new_name)

        # remove '|' at the end of each row
        os.system('chmod -R +rw .')
        pool = Pool(processes=multiprocessing.cpu_count()*2)
        for partition_file in partition_files:
            if platform.system() == "Darwin":
                cmd_remove_end = 'sed -i \'\' \'s/.$//\' {}'.format(partition_file)
            else:
                cmd_remove_end = 'sed -i \'s/.$//\' {}'.format(partition_file)
            pool.apply_async(run_command, args=(cmd_remove_end,))
        pool.close()
        pool.join()

        # add column names
        os.system('chmod -R +rw .')
        pool = Pool(processes=multiprocessing.cpu_count()*2)
        for partition_file in partition_files:
            if platform.system() == "Darwin":
                cmd_add_column_names = 'sed -i \'\' \'1s/^/{}\\\'$\'\\n/\' {}'.format(column_names, partition_file)
            else:
                cmd_add_column_names = 'sed -i \'1i {}\' {}'.format(column_names, partition_file)
            pool.apply_async(run_command, args=(cmd_add_column_names,))
        pool.close()
        pool.join()

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


def gen_partition(table, num_partitions, partition):
    if num_partitions > 1:
        os.system('./dbgen -s {} -C {} -S {} -T {} -f'.format(sf, num_partitions, partition, table_to_flag[table]))
    else:
        os.system('./dbgen -s {} -T {} -f'.format(sf, table_to_flag[table]))


def gen_partitions():
    pool = Pool(processes=multiprocessing.cpu_count()*2)
    for table in tables:
        num_partitions = num_partitions_dict[table]
        for partition in range(1, num_partitions + 1):
            pool.apply_async(gen_partition, args=(table, num_partitions, partition,))
    pool.close()
    pool.join()


def gen():
    # generate data
    if direct_gen_partitions:
        gen_partitions()
        print("Tables generated.")
    else:
        os.system('./dbgen -s {}'.format(sf))
        print("Tables generated.")

    # format for each table
    for table in tables:
        print("Formatting " + table + "... ", end='', flush=True)
        format_data_for_table(table, column_names_dict[table], num_partitions_dict[table])
        print('done')
    print("Tables formatted.")


if __name__ == "__main__":
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
                    '|L_LINESTATUS|L_SHIPDATE|L_COMMITDATE|L_RECEIPTDATE|L_SHIPINSTRUCT|L_SHIPMODE|L_COMMENT'
    }

    # pick appropriate split tool
    if platform.system() == "Darwin":
        split_func = "gsplit"
    else:
        split_func = "split"

    # built-in partition generator cannot generate multiple partitions for "nation" or "region"
    if num_partitions_dict['nation'] > 1 or num_partitions_dict['region'] > 1:
        direct_gen_partitions = False

    # create the directory to put data
    data_dir = 'tpch-sf' + str(sf)
    os.system('rm -rf {}'.format(data_dir))
    os.system('mkdir {}'.format(data_dir))

    # generate
    gen()

    # clear
    os.system('rm -f *.tbl')
