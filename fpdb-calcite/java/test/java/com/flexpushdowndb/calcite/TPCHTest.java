package com.flexpushdowndb.calcite;

import org.junit.jupiter.api.Test;

public class TPCHTest {

  @Test
  public void testTPCH_Q01() throws Exception {
    TestUtil.test("tpch-sf0.01/csv", "tpch/original/01.sql", true);
  }

  @Test
  public void testTPCH_Q02() throws Exception {
    TestUtil.test("tpch-sf0.01/csv", "tpch/original/02.sql", true);
  }

  @Test
  public void testTPCH_Q03() throws Exception {
    TestUtil.test("tpch-sf0.01/csv", "tpch/original/03.sql", true);
  }

  @Test
  public void testTPCH_Q04() throws Exception {
    TestUtil.test("tpch-sf0.01/csv", "tpch/original/04.sql", true);
  }

  @Test
  // FIXME: not considering key-foreign key constraint with filtering leads to a bad query plan
  //  currently manually specify the join order
  public void testTPCH_Q05() throws Exception {
    TestUtil.testNoHeuristicJoinOrdering("tpch-sf0.01/csv", "tpch/original/05.sql", true);
  }

  @Test
  public void testTPCH_Q06() throws Exception {
    TestUtil.test("tpch-sf0.01/csv", "tpch/original/06.sql", true);
  }

  @Test
  public void testTPCH_Q07() throws Exception {
    TestUtil.test("tpch-sf0.01/csv", "tpch/original/07.sql", true);
  }

  @Test
  public void testTPCH_Q08() throws Exception {
    TestUtil.test("tpch-sf0.01/csv", "tpch/original/08.sql", true);
  }

  @Test
  public void testTPCH_Q09() throws Exception {
    TestUtil.test("tpch-sf0.01/csv", "tpch/original/09.sql", true);
  }

  @Test
  public void testTPCH_Q10() throws Exception {
    TestUtil.test("tpch-sf0.01/csv", "tpch/original/10.sql", true);
  }

  @Test
  public void testTPCH_Q11() throws Exception {
    TestUtil.test("tpch-sf0.01/csv", "tpch/original/11.sql", true);
  }

  @Test
  public void testTPCH_Q12() throws Exception {
    TestUtil.test("tpch-sf0.01/csv", "tpch/original/12.sql", true);
  }

  @Test
  public void testTPCH_Q13() throws Exception {
    TestUtil.test("tpch-sf0.01/csv", "tpch/original/13.sql", true);
  }

  @Test
  public void testTPCH_Q14() throws Exception {
    TestUtil.test("tpch-sf0.01/csv", "tpch/original/14.sql", true);
  }

  @Test
  public void testTPCH_Q15() throws Exception {
    TestUtil.test("tpch-sf0.01/csv", "tpch/original/15.sql", true);
  }

  @Test
  public void testTPCH_Q16() throws Exception {
    TestUtil.test("tpch-sf0.01/csv", "tpch/original/16.sql", true);
  }

  @Test
  public void testTPCH_Q17() throws Exception {
    TestUtil.test("tpch-sf0.01/csv", "tpch/original/17.sql", true);
  }

  @Test
  public void testTPCH_Q18() throws Exception {
    TestUtil.test("tpch-sf0.01/csv", "tpch/original/18.sql", true);
  }

  @Test
  public void testTPCH_Q19() throws Exception {
    TestUtil.test("tpch-sf0.01/csv", "tpch/original/19.sql", true);
  }

  @Test
  public void testTPCH_Q20() throws Exception {
    TestUtil.test("tpch-sf0.01/csv", "tpch/original/20.sql", true);
  }

  @Test
  public void testTPCH_Q21() throws Exception {
    TestUtil.test("tpch-sf0.01/csv", "tpch/original/21.sql", true);
  }

  @Test
  public void testTPCH_Q22() throws Exception {
    TestUtil.test("tpch-sf0.01/csv", "tpch/original/22.sql", true);
  }

  // for 'tpch-sf0.01-1-node-hash-part' used in the following tests,
  // 'lineitem' and 'orders' are co-located at 'l_orderkey = o.o_orderkey'
  @Test
  public void testCoHashJoin_TPCH_Q01() throws Exception {
    TestUtil.test("tpch-sf0.01-1-node-hash-part/parquet", "tpch/original/01.sql", true);
  }

  @Test
  public void testCoHashJoin_TPCH_Q02() throws Exception {
    TestUtil.test("tpch-sf0.01-1-node-hash-part/parquet", "tpch/original/02.sql", true);
  }

  // has pushable co-located join
  @Test
  public void testCoHashJoin_TPCH_Q03() throws Exception {
    TestUtil.test("tpch-sf0.01-1-node-hash-part/parquet", "tpch/original/03.sql", true);
  }

  @Test
  // has pushable co-located join
  public void testCoHashJoin_TPCH_Q04() throws Exception {
    TestUtil.test("tpch-sf0.01-1-node-hash-part/parquet", "tpch/original/04.sql", true);
  }

  @Test
  // FIXME: it actually has pushable co-located join between 'lineitem' and 'orders', but only planned as that
  //  when COLOCATE_JOIN_REDUCTION_FACTOR is set to extremely high or just return a extremely small row count
  //  for that join
  public void testCoHashJoin_TPCH_Q05() throws Exception {
    TestUtil.test("tpch-sf0.01-1-node-hash-part/parquet", "tpch/original/05.sql", true);
  }

  @Test
  public void testCoHashJoin_TPCH_Q06() throws Exception {
    TestUtil.test("tpch-sf0.01-1-node-hash-part/parquet", "tpch/original/06.sql", true);
  }

  @Test
  // has pushable co-located join
  public void testCoHashJoin_TPCH_Q07() throws Exception {
    TestUtil.test("tpch-sf0.01-1-node-hash-part/parquet", "tpch/original/07.sql", true);
  }

  @Test
  // has pushable co-located join
  public void testCoHashJoin_TPCH_Q08() throws Exception {
    TestUtil.test("tpch-sf0.01-1-node-hash-part/parquet", "tpch/original/08.sql", true);
  }

  @Test
  // has pushable co-located join
  public void testCoHashJoin_TPCH_Q09() throws Exception {
    TestUtil.test("tpch-sf0.01-1-node-hash-part/parquet", "tpch/original/09.sql", true);
  }

  @Test
  // has pushable co-located join
  public void testCoHashJoin_TPCH_Q10() throws Exception {
    TestUtil.test("tpch-sf0.01-1-node-hash-part/parquet", "tpch/original/10.sql", true);
  }

  @Test
  public void testCoHashJoin_TPCH_Q11() throws Exception {
    TestUtil.test("tpch-sf0.01-1-node-hash-part/parquet", "tpch/original/11.sql", true);
  }

  @Test
  // has pushable co-located join
  public void testCoHashJoin_TPCH_Q12() throws Exception {
    TestUtil.test("tpch-sf0.01-1-node-hash-part/parquet", "tpch/original/12.sql", true);
  }

  @Test
  public void testCoHashJoin_TPCH_Q13() throws Exception {
    TestUtil.test("tpch-sf0.01-1-node-hash-part/parquet", "tpch/original/13.sql", true);
  }

  @Test
  public void testCoHashJoin_TPCH_Q14() throws Exception {
    TestUtil.test("tpch-sf0.01-1-node-hash-part/parquet", "tpch/original/14.sql", true);
  }

  @Test
  public void testCoHashJoin_TPCH_Q15() throws Exception {
    TestUtil.test("tpch-sf0.01-1-node-hash-part/parquet", "tpch/original/15.sql", true);
  }

  @Test
  public void testCoHashJoin_TPCH_Q16() throws Exception {
    TestUtil.test("tpch-sf0.01-1-node-hash-part/parquet", "tpch/original/16.sql", true);
  }

  @Test
  public void testCoHashJoin_TPCH_Q17() throws Exception {
    TestUtil.test("tpch-sf0.01-1-node-hash-part/parquet", "tpch/original/17.sql", true);
  }

  @Test
  // has pushable co-located join
  public void testCoHashJoin_TPCH_Q18() throws Exception {
    TestUtil.test("tpch-sf0.01-1-node-hash-part/parquet", "tpch/original/18.sql", true);
  }

  @Test
  public void testCoHashJoin_TPCH_Q19() throws Exception {
    TestUtil.test("tpch-sf0.01-1-node-hash-part/parquet", "tpch/original/19.sql", true);
  }

  @Test
  public void testCoHashJoin_TPCH_Q20() throws Exception {
    TestUtil.test("tpch-sf0.01-1-node-hash-part/parquet", "tpch/original/20.sql", true);
  }

  @Test
  // has pushable co-located join
  public void testCoHashJoin_TPCH_Q21() throws Exception {
    TestUtil.test("tpch-sf0.01-1-node-hash-part/parquet", "tpch/original/21.sql", true);
  }

  @Test
  public void testCoHashJoin_TPCH_Q22() throws Exception {
    TestUtil.test("tpch-sf0.01-1-node-hash-part/parquet", "tpch/original/22.sql", true);
  }
}
