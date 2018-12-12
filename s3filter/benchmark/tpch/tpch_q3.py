# -*- coding: utf-8 -*-
"""TPCH Q3 Benchmarks

"""

from s3filter.benchmark.tpch import tpch_results, tpch_q3_baseline_join, tpch_q3_filtered_join, tpch_q3_bloom_join
from s3filter.benchmark.tpch.run_tpch import start_capture, end_capture


def main(sf, customer_parts, customer_sharded, order_parts, order_sharded, lineitem_parts, lineitem_sharded,
         other_parts, fp_rate,
         expected_result, trial):

    out_file, path = start_capture('tpch_q3', sf, 'baseline', trial)

    tpch_q3_baseline_join.main(sf, customer_parts, customer_sharded, order_parts, order_sharded, lineitem_parts,
                               lineitem_sharded, other_parts, expected_result)

    end_capture(out_file, path)
    out_file, path = start_capture('tpch_q3', sf, 'filtered', trial)

    tpch_q3_filtered_join.main(sf, customer_parts, customer_sharded, order_parts, order_sharded, lineitem_parts,
                               lineitem_sharded, other_parts, expected_result)

    end_capture(out_file, path)
    out_file, path = start_capture('tpch_q3', sf, 'bloom', trial)

    tpch_q3_bloom_join.main(sf, customer_parts, customer_sharded, order_parts, order_sharded, lineitem_parts,
                            lineitem_sharded, fp_rate, other_parts, expected_result)

    end_capture(out_file, path)


if __name__ == "__main__":
    main(1, 4, False, 4, False, 2, False, 2, 0.01, tpch_results.q3_sf1_expected_result, 1)
