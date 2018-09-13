from collections import OrderedDict

from s3filter.op.aggregate import Aggregate
from s3filter.op.aggregate_expression import AggregateExpression
from s3filter.op.collate import Collate
from s3filter.op.filter import Filter
from s3filter.op.hash_join_build import HashJoinBuild
from s3filter.op.hash_join_probe import HashJoinProbe
from s3filter.op.join_expression import JoinExpression
from s3filter.op.map import Map
from s3filter.op.operator_connector import connect_many_to_many, connect_all_to_all, connect_many_to_one, \
    connect_one_to_one
from s3filter.op.predicate_expression import PredicateExpression
from s3filter.op.project import ProjectExpression, Project
from s3filter.op.sql_table_scan import SQLTableScan
from s3filter.plan.query_plan import QueryPlan
from s3filter.query.tpch import get_file_key
from s3filter.query.tpch_q19 import get_sql_suffix


class SyntheticJoinSettings(object):

    def __init__(self, parallel,
                 use_pandas,
                 secure,
                 use_native,
                 buffer_size,
                 table_A_key,
                 table_A_parts,
                 table_A_sharded,
                 table_A_field_names,
                 table_A_AB_join_key,
                 table_B_key,
                 table_B_parts,
                 table_B_sharded,
                 table_B_field_names,
                 table_B_AB_join_key,
                 table_B_BC_join_key,
                 table_C_key,
                 table_C_parts,
                 table_C_sharded,
                 table_C_field_names,
                 table_C_BC_join_key):
        self.parallel = parallel
        self.use_pandas = use_pandas
        self.secure = secure
        self.use_native = use_native
        self.buffer_size = buffer_size
        self.table_A_key = table_A_key
        self.table_A_parts = table_A_parts
        self.table_A_sharded = table_A_sharded
        self.table_A_field_names = table_A_field_names
        self.table_A_AB_join_key = table_A_AB_join_key
        self.table_B_key = table_B_key
        self.table_B_parts = table_B_parts
        self.table_B_sharded = table_B_sharded
        self.table_B_field_names = table_B_field_names
        self.table_B_AB_join_key = table_B_AB_join_key
        self.table_B_BC_join_key = table_B_BC_join_key
        self.table_C_key = table_C_key
        self.table_C_parts = table_C_parts
        self.table_C_sharded = table_C_sharded
        self.table_C_field_names = table_C_field_names
        self.table_C_BC_join_key = table_C_BC_join_key


class SyntheticBaselineJoinSettings(SyntheticJoinSettings):

    def __init__(self, parallel,
                 use_pandas,
                 secure,
                 use_native,
                 buffer_size,
                 table_A_key,
                 table_A_parts,
                 table_A_sharded,
                 table_A_field_names,
                 table_A_filter_fn,
                 table_A_AB_join_key,
                 table_B_key,
                 table_B_parts,
                 table_B_sharded,
                 table_B_field_names,
                 table_B_AB_join_key,
                 table_B_BC_join_key,
                 table_C_key,
                 table_C_parts,
                 table_C_sharded,
                 table_C_field_names,
                 table_C_BC_join_key):
        super(SyntheticBaselineJoinSettings, self).__init__(parallel,
                                                            use_pandas,
                                                            secure,
                                                            use_native,
                                                            buffer_size,
                                                            table_A_key,
                                                            table_A_parts,
                                                            table_A_sharded,
                                                            table_A_field_names,
                                                            table_A_AB_join_key,
                                                            table_B_key,
                                                            table_B_parts,
                                                            table_B_sharded,
                                                            table_B_field_names,
                                                            table_B_AB_join_key,
                                                            table_B_BC_join_key,
                                                            table_C_key,
                                                            table_C_parts,
                                                            table_C_sharded,
                                                            table_C_field_names,
                                                            table_C_BC_join_key)

        self.table_A_filter_fn = table_A_filter_fn


class SyntheticFilteredJoinSettings(SyntheticJoinSettings):

    def __init__(self, parallel,
                 use_pandas,
                 secure,
                 use_native,
                 buffer_size,
                 table_A_key,
                 table_A_parts,
                 table_A_sharded,
                 table_A_field_names,
                 table_A_filter_sql,
                 table_A_AB_join_key,
                 table_B_key,
                 table_B_parts,
                 table_B_sharded,
                 table_B_field_names,
                 table_B_AB_join_key,
                 table_B_BC_join_key,
                 table_C_key,
                 table_C_parts,
                 table_C_sharded,
                 table_C_field_names,
                 table_C_BC_join_key):
        super(SyntheticFilteredJoinSettings, self).__init__(parallel,
                                                            use_pandas,
                                                            secure,
                                                            use_native,
                                                            buffer_size,
                                                            table_A_key,
                                                            table_A_parts,
                                                            table_A_sharded,
                                                            table_A_field_names,
                                                            table_A_AB_join_key,
                                                            table_B_key,
                                                            table_B_parts,
                                                            table_B_sharded,
                                                            table_B_field_names,
                                                            table_B_AB_join_key,
                                                            table_B_BC_join_key,
                                                            table_C_key,
                                                            table_C_parts,
                                                            table_C_sharded,
                                                            table_C_field_names,
                                                            table_C_BC_join_key)

        self.table_A_filter_sql = table_A_filter_sql


class SyntheticBloomJoinSettings(SyntheticJoinSettings):

    def __init__(self, parallel,
                 use_pandas,
                 secure,
                 use_native,
                 buffer_size,
                 table_A_key,
                 table_A_parts,
                 table_A_sharded,
                 table_A_field_names,
                 table_A_filter_sql,
                 table_A_AB_join_key,
                 table_B_key,
                 table_B_parts,
                 table_B_sharded,
                 table_B_field_names,
                 table_B_AB_join_key,
                 table_B_BC_join_key,
                 table_C_key,
                 table_C_parts,
                 table_C_sharded,
                 table_C_field_names,
                 table_C_BC_join_key):
        super(SyntheticBloomJoinSettings, self).__init__(parallel,
                                                         use_pandas,
                                                         secure,
                                                         use_native,
                                                         buffer_size,
                                                         table_A_key,
                                                         table_A_parts,
                                                         table_A_sharded,
                                                         table_A_field_names,
                                                         table_A_AB_join_key,
                                                         table_B_key,
                                                         table_B_parts,
                                                         table_B_sharded,
                                                         table_B_field_names,
                                                         table_B_AB_join_key,
                                                         table_B_BC_join_key,
                                                         table_C_key,
                                                         table_C_parts,
                                                         table_C_sharded,
                                                         table_C_field_names,
                                                         table_C_BC_join_key)

        self.table_A_filter_sql = table_A_filter_sql


class SyntheticSemiJoinSettings(SyntheticJoinSettings):

    def __init__(self, parallel,
                 use_pandas,
                 secure,
                 use_native,
                 buffer_size,
                 table_A_key,
                 table_A_parts,
                 table_A_sharded,
                 table_A_field_names,
                 table_A_AB_join_key,
                 table_B_key,
                 table_B_parts,
                 table_B_sharded,
                 table_B_field_names,
                 table_B_AB_join_key,
                 table_B_BC_join_key,
                 table_C_key,
                 table_C_parts,
                 table_C_sharded,
                 table_C_field_names,
                 table_C_BC_join_key):
        super(SyntheticSemiJoinSettings, self).__init__(parallel,
                                                        use_pandas,
                                                        secure,
                                                        use_native,
                                                        buffer_size,
                                                        table_A_key,
                                                        table_A_parts,
                                                        table_A_sharded,
                                                        table_A_field_names,
                                                        table_A_AB_join_key,
                                                        table_B_key,
                                                        table_B_parts,
                                                        table_B_sharded,
                                                        table_B_field_names,
                                                        table_B_AB_join_key,
                                                        table_B_BC_join_key,
                                                        table_C_key,
                                                        table_C_parts,
                                                        table_C_sharded,
                                                        table_C_field_names,
                                                        table_C_BC_join_key)
