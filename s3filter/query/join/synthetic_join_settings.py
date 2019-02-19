class SyntheticJoinSettings(object):

    def __init__(self, parallel,
                 use_pandas,
                 secure,
                 use_native,
                 buffer_size,
                 use_shared_mem,
                 shared_memory_size,
                 format_,
                 sf,
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
                 table_B_detail_field_name,
                 table_C_key,
                 table_C_parts,
                 table_C_sharded,
                 table_C_field_names,
                 table_C_BC_join_key,
                 table_C_detail_field_name,
                 other_parts):
        self.parallel = parallel
        self.use_pandas = use_pandas
        self.secure = secure
        self.use_native = use_native
        self.buffer_size = buffer_size
        self.use_shared_mem = use_shared_mem
        self.shared_memory_size = shared_memory_size
        self.format_ = format_
        self.sf = sf
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
        self.table_B_detail_field_name = table_B_detail_field_name
        self.table_C_key = table_C_key
        self.table_C_parts = table_C_parts
        self.table_C_sharded = table_C_sharded
        self.table_C_field_names = table_C_field_names
        self.table_C_BC_join_key = table_C_BC_join_key
        self.table_C_detail_field_name = table_C_detail_field_name
        self.other_parts = other_parts


class SyntheticBaselineJoinSettings(SyntheticJoinSettings):

    def __init__(self, parallel,
                 use_pandas,
                 secure,
                 use_native,
                 buffer_size,
                 use_shared_mem,
                 shared_memory_size,
                 format_,
                 sf,
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
                 table_B_filter_fn,
                 table_B_AB_join_key,
                 table_B_BC_join_key,
                 table_B_detail_field_name,
                 table_C_key,
                 table_C_parts,
                 table_C_sharded,
                 table_C_field_names,
                 table_C_filter_fn,
                 table_C_BC_join_key,
                 table_C_detail_field_name,
                 other_parts):
        super(SyntheticBaselineJoinSettings, self).__init__(parallel,
                                                            use_pandas,
                                                            secure,
                                                            use_native,
                                                            buffer_size,
                                                            use_shared_mem,
                                                            shared_memory_size,
                                                            format_,
                                                            sf,
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
                                                            table_B_detail_field_name,
                                                            table_C_key,
                                                            table_C_parts,
                                                            table_C_sharded,
                                                            table_C_field_names,
                                                            table_C_BC_join_key,
                                                            table_C_detail_field_name,
                                                            other_parts)

        self.table_A_filter_fn = table_A_filter_fn
        self.table_B_filter_fn = table_B_filter_fn
        self.table_C_filter_fn = table_C_filter_fn


class SyntheticFilteredJoinSettings(SyntheticJoinSettings):

    def __init__(self, parallel,
                 use_pandas,
                 secure,
                 use_native,
                 buffer_size,
                 use_shared_mem,
                 shared_memory_size,
                 format_,
                 sf,
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
                 table_B_filter_sql,
                 table_B_AB_join_key,
                 table_B_BC_join_key,
                 table_B_detail_field_name,
                 table_C_key,
                 table_C_parts,
                 table_C_sharded,
                 table_C_field_names,
                 table_C_filter_sql,
                 table_C_BC_join_key,
                 table_C_detail_field_name,
                 other_parts):
        super(SyntheticFilteredJoinSettings, self).__init__(parallel,
                                                            use_pandas,
                                                            secure,
                                                            use_native,
                                                            buffer_size,
                                                            use_shared_mem,
                                                            shared_memory_size,
                                                            format_,
                                                            sf,
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
                                                            table_B_detail_field_name,
                                                            table_C_key,
                                                            table_C_parts,
                                                            table_C_sharded,
                                                            table_C_field_names,
                                                            table_C_BC_join_key,
                                                            table_C_detail_field_name,
                                                            other_parts)

        self.table_A_filter_sql = table_A_filter_sql
        self.table_B_filter_sql = table_B_filter_sql
        self.table_C_filter_sql = table_C_filter_sql


class SyntheticBloomJoinSettings(SyntheticJoinSettings):

    def __init__(self, parallel,
                 use_pandas,
                 secure,
                 use_native,
                 buffer_size,
                 use_shared_mem,
                 shared_memory_size,
                 format_,
                 sf,
                 fp_rate,
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
                 table_B_filter_sql,
                 table_B_AB_join_key,
                 table_B_BC_join_key,
                 table_B_detail_field_name,
                 table_C_key,
                 table_C_parts,
                 table_C_sharded,
                 table_C_field_names,
                 table_C_filter_sql,
                 table_C_BC_join_key,
                 table_C_detail_field_name,
                 other_parts):
        super(SyntheticBloomJoinSettings, self).__init__(parallel,
                                                         use_pandas,
                                                         secure,
                                                         use_native,
                                                         buffer_size,
                                                         use_shared_mem,
                                                         shared_memory_size,
                                                         format_,
                                                         sf,
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
                                                         table_B_detail_field_name,
                                                         table_C_key,
                                                         table_C_parts,
                                                         table_C_sharded,
                                                         table_C_field_names,
                                                         table_C_BC_join_key,
                                                         table_C_detail_field_name,
                                                         other_parts)

        self.table_A_filter_sql = table_A_filter_sql
        self.table_B_filter_sql = table_B_filter_sql
        self.table_C_filter_sql = table_C_filter_sql
        self.fp_rate = fp_rate


class SyntheticSemiJoinSettings(SyntheticJoinSettings):

    def __init__(self, parallel,
                 use_pandas,
                 secure,
                 use_native,
                 buffer_size,
                 use_shared_mem,
                 shared_memory_size,
                 format_,
                 sf,
                 fp_rate,
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
                 table_B_filter_sql,
                 table_B_AB_join_key,
                 table_B_BC_join_key,
                 table_B_primary_key,
                 table_B_detail_field_name,
                 table_C_key,
                 table_C_parts,
                 table_C_sharded,
                 table_C_field_names,
                 table_C_filter_sql,
                 table_C_BC_join_key,
                 table_C_primary_key,
                 table_C_detail_field_name,
                 other_parts):
        super(SyntheticSemiJoinSettings, self).__init__(parallel,
                                                        use_pandas,
                                                        secure,
                                                        use_native,
                                                        buffer_size,
                                                        use_shared_mem,
                                                        shared_memory_size,
                                                        format_,
                                                        sf,
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
                                                        table_B_detail_field_name,
                                                        table_C_key,
                                                        table_C_parts,
                                                        table_C_sharded,
                                                        table_C_field_names,
                                                        table_C_BC_join_key,
                                                        table_C_detail_field_name,
                                                        other_parts)

        self.table_A_filter_sql = table_A_filter_sql
        self.table_B_filter_sql = table_B_filter_sql
        self.table_C_filter_sql = table_C_filter_sql
        self.table_B_primary_key = table_B_primary_key
        self.table_C_primary_key = table_C_primary_key

        self.fp_rate = fp_rate
