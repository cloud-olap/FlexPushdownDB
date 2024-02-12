//
// Created by Yifei Yang on 12/1/22.
//

#include <doctest/doctest.h>
#include "TestUtil.h"
#include "Globals.h"

/**
 * SSB test (multiple compute nodes, FPDB store)
 *
 * Start Calcite server on the coordinator (locally), CAF server on all executors (remote nodes),
 * and FPDB store server before running this (see README for more details)
 */
namespace fpdb::main::test {

#define SKIP_SUITE false

void run_ssb_sf100_4_node_hash_part_fpdb_store_distributed_parquet_pullup(const std::string &queryFileName) {
  REQUIRE(TestUtil::e2eNoStartCalciteServerNoHeuristicJoinOrdering("ssb-sf100-4-node-hash-part/parquet/",
                                                                   {queryFileName},
                                                                   PARALLEL_DIST_SF100,
                                                                   true,
                                                                   ObjStoreType::FPDB_STORE));
}

void run_ssb_sf100_4_node_hash_part_fpdb_store_distributed_parquet_pushdown_only(const std::string &queryFileName) {
  REQUIRE(TestUtil::e2eNoStartCalciteServerNoHeuristicJoinOrdering("ssb-sf100-4-node-hash-part/parquet/",
                                                                   {queryFileName},
                                                                   PARALLEL_DIST_SF100,
                                                                   true,
                                                                   ObjStoreType::FPDB_STORE,
                                                                   Mode::pushdownOnlyMode()));
}

TEST_SUITE ("ssb-sf100-4-node-hash-part-fpdb-store-distributed-parquet-pullup" * doctest::skip(SKIP_SUITE)) {

TEST_CASE ("ssb-sf100-4-node-hash-part-fpdb-store-distributed-parquet-pullup-1.1" * doctest::skip(false || SKIP_SUITE)) {
  run_ssb_sf100_4_node_hash_part_fpdb_store_distributed_parquet_pullup("ssb/original/1.1.sql");
}

TEST_CASE ("ssb-sf100-4-node-hash-part-fpdb-store-distributed-parquet-pullup-1.2" * doctest::skip(false || SKIP_SUITE)) {
  run_ssb_sf100_4_node_hash_part_fpdb_store_distributed_parquet_pullup("ssb/original/1.2.sql");
}

TEST_CASE ("ssb-sf100-4-node-hash-part-fpdb-store-distributed-parquet-pullup-1.3" * doctest::skip(false || SKIP_SUITE)) {
  run_ssb_sf100_4_node_hash_part_fpdb_store_distributed_parquet_pullup("ssb/original/1.3.sql");
}

TEST_CASE ("ssb-sf100-4-node-hash-part-fpdb-store-distributed-parquet-pullup-2.1" * doctest::skip(false || SKIP_SUITE)) {
  run_ssb_sf100_4_node_hash_part_fpdb_store_distributed_parquet_pullup("ssb/original/2.1.sql");
}

TEST_CASE ("ssb-sf100-4-node-hash-part-fpdb-store-distributed-parquet-pullup-2.2" * doctest::skip(false || SKIP_SUITE)) {
  run_ssb_sf100_4_node_hash_part_fpdb_store_distributed_parquet_pullup("ssb/original/2.2.sql");
}

TEST_CASE ("ssb-sf100-4-node-hash-part-fpdb-store-distributed-parquet-pullup-2.3" * doctest::skip(false || SKIP_SUITE)) {
  run_ssb_sf100_4_node_hash_part_fpdb_store_distributed_parquet_pullup("ssb/original/2.3.sql");
}

TEST_CASE ("ssb-sf100-4-node-hash-part-fpdb-store-distributed-parquet-pullup-3.1" * doctest::skip(false || SKIP_SUITE)) {
  run_ssb_sf100_4_node_hash_part_fpdb_store_distributed_parquet_pullup("ssb/original/3.1.sql");
}

TEST_CASE ("ssb-sf100-4-node-hash-part-fpdb-store-distributed-parquet-pullup-3.2" * doctest::skip(false || SKIP_SUITE)) {
  run_ssb_sf100_4_node_hash_part_fpdb_store_distributed_parquet_pullup("ssb/original/3.2.sql");
}

TEST_CASE ("ssb-sf100-4-node-hash-part-fpdb-store-distributed-parquet-pullup-3.3" * doctest::skip(false || SKIP_SUITE)) {
  run_ssb_sf100_4_node_hash_part_fpdb_store_distributed_parquet_pullup("ssb/original/3.3.sql");
}

TEST_CASE ("ssb-sf100-4-node-hash-part-fpdb-store-distributed-parquet-pullup-3.4" * doctest::skip(false || SKIP_SUITE)) {
  run_ssb_sf100_4_node_hash_part_fpdb_store_distributed_parquet_pullup("ssb/original/3.4.sql");
}

TEST_CASE ("ssb-sf100-4-node-hash-part-fpdb-store-distributed-parquet-pullup-4.1" * doctest::skip(false || SKIP_SUITE)) {
  run_ssb_sf100_4_node_hash_part_fpdb_store_distributed_parquet_pullup("ssb/original/4.1.sql");
}

TEST_CASE ("ssb-sf100-4-node-hash-part-fpdb-store-distributed-parquet-pullup-4.2" * doctest::skip(false || SKIP_SUITE)) {
  run_ssb_sf100_4_node_hash_part_fpdb_store_distributed_parquet_pullup("ssb/original/4.2.sql");
}

TEST_CASE ("ssb-sf100-4-node-hash-part-fpdb-store-distributed-parquet-pullup-4.3" * doctest::skip(false || SKIP_SUITE)) {
  run_ssb_sf100_4_node_hash_part_fpdb_store_distributed_parquet_pullup("ssb/original/4.3.sql");
}

}

TEST_SUITE ("ssb-sf100-4-node-hash-part-fpdb-store-distributed-parquet-pushdown-only" * doctest::skip(SKIP_SUITE)) {

TEST_CASE ("ssb-sf100-4-node-hash-part-fpdb-store-distributed-parquet-pushdown-only-1.1" * doctest::skip(false || SKIP_SUITE)) {
  run_ssb_sf100_4_node_hash_part_fpdb_store_distributed_parquet_pushdown_only("ssb/original/1.1.sql");
}

TEST_CASE ("ssb-sf100-4-node-hash-part-fpdb-store-distributed-parquet-pushdown-only-1.2" * doctest::skip(false || SKIP_SUITE)) {
  run_ssb_sf100_4_node_hash_part_fpdb_store_distributed_parquet_pushdown_only("ssb/original/1.2.sql");
}

TEST_CASE ("ssb-sf100-4-node-hash-part-fpdb-store-distributed-parquet-pushdown-only-1.3" * doctest::skip(false || SKIP_SUITE)) {
  run_ssb_sf100_4_node_hash_part_fpdb_store_distributed_parquet_pushdown_only("ssb/original/1.3.sql");
}

TEST_CASE ("ssb-sf100-4-node-hash-part-fpdb-store-distributed-parquet-pushdown-only-2.1" * doctest::skip(false || SKIP_SUITE)) {
  run_ssb_sf100_4_node_hash_part_fpdb_store_distributed_parquet_pushdown_only("ssb/original/2.1.sql");
}

TEST_CASE ("ssb-sf100-4-node-hash-part-fpdb-store-distributed-parquet-pushdown-only-2.2" * doctest::skip(false || SKIP_SUITE)) {
  run_ssb_sf100_4_node_hash_part_fpdb_store_distributed_parquet_pushdown_only("ssb/original/2.2.sql");
}

TEST_CASE ("ssb-sf100-4-node-hash-part-fpdb-store-distributed-parquet-pushdown-only-2.3" * doctest::skip(false || SKIP_SUITE)) {
  run_ssb_sf100_4_node_hash_part_fpdb_store_distributed_parquet_pushdown_only("ssb/original/2.3.sql");
}

TEST_CASE ("ssb-sf100-4-node-hash-part-fpdb-store-distributed-parquet-pushdown-only-3.1" * doctest::skip(false || SKIP_SUITE)) {
  run_ssb_sf100_4_node_hash_part_fpdb_store_distributed_parquet_pushdown_only("ssb/original/3.1.sql");
}

TEST_CASE ("ssb-sf100-4-node-hash-part-fpdb-store-distributed-parquet-pushdown-only-3.2" * doctest::skip(false || SKIP_SUITE)) {
  run_ssb_sf100_4_node_hash_part_fpdb_store_distributed_parquet_pushdown_only("ssb/original/3.2.sql");
}

TEST_CASE ("ssb-sf100-4-node-hash-part-fpdb-store-distributed-parquet-pushdown-only-3.3" * doctest::skip(false || SKIP_SUITE)) {
  run_ssb_sf100_4_node_hash_part_fpdb_store_distributed_parquet_pushdown_only("ssb/original/3.3.sql");
}

TEST_CASE ("ssb-sf100-4-node-hash-part-fpdb-store-distributed-parquet-pushdown-only-3.4" * doctest::skip(false || SKIP_SUITE)) {
  run_ssb_sf100_4_node_hash_part_fpdb_store_distributed_parquet_pushdown_only("ssb/original/3.4.sql");
}

TEST_CASE ("ssb-sf100-4-node-hash-part-fpdb-store-distributed-parquet-pushdown-only-4.1" * doctest::skip(false || SKIP_SUITE)) {
  run_ssb_sf100_4_node_hash_part_fpdb_store_distributed_parquet_pushdown_only("ssb/original/4.1.sql");
}

TEST_CASE ("ssb-sf100-4-node-hash-part-fpdb-store-distributed-parquet-pushdown-only-4.2" * doctest::skip(false || SKIP_SUITE)) {
  run_ssb_sf100_4_node_hash_part_fpdb_store_distributed_parquet_pushdown_only("ssb/original/4.2.sql");
}

TEST_CASE ("ssb-sf100-4-node-hash-part-fpdb-store-distributed-parquet-pushdown-only-4.3" * doctest::skip(false || SKIP_SUITE)) {
  run_ssb_sf100_4_node_hash_part_fpdb_store_distributed_parquet_pushdown_only("ssb/original/4.3.sql");
}

}

}
