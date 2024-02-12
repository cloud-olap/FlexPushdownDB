package com.flexpushdowndb.calcite;

import org.junit.jupiter.api.Test;

public class MultiQueryTest {
  
  @Test
  public void testMultiQuerySameSchema() throws Exception {
    TestUtil.test("ssb-sf1-sortlineorder/csv", "ssb/original/2.1.sql", false);
    TestUtil.test("ssb-sf1-sortlineorder/csv", "ssb/original/2.1.sql", false);
  }

  @Test
  public void testMultiQueryDiffSchema() throws Exception {
    TestUtil.test("ssb-sf1-sortlineorder/csv", "ssb/original/2.1.sql", false);
    TestUtil.test("ssb-sf10-sortlineorder/csv", "ssb/original/2.1.sql", false);
  }
}
