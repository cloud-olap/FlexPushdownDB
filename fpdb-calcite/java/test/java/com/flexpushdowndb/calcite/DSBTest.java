package com.flexpushdowndb.calcite;

import org.junit.jupiter.api.Test;

public class DSBTest {

  @Test
  public void testDSB_spj_Q13() throws Exception {
    TestUtil.test("dsb-sf1/parquet", "dsb/original/spj/q13_spj.sql", true);
  }

  @Test
  public void testDSB_spj_Q18() throws Exception {
    TestUtil.test("dsb-sf1/parquet", "dsb/original/spj/q18_spj.sql", true);
  }

  @Test
  public void testDSB_spj_Q19() throws Exception {
    TestUtil.test("dsb-sf1/parquet", "dsb/original/spj/q19_spj.sql", true);
  }

  @Test
  public void testDSB_spj_Q25() throws Exception {
    TestUtil.test("dsb-sf1/parquet", "dsb/original/spj/q25_spj.sql", true);
  }

  @Test
  public void testDSB_spj_Q27() throws Exception {
    TestUtil.test("dsb-sf1/parquet", "dsb/original/spj/q27_spj.sql", true);
  }

  @Test
  public void testDSB_spj_Q40() throws Exception {
    TestUtil.test("dsb-sf1/parquet", "dsb/original/spj/q40_spj.sql", true);
  }

  @Test
  public void testDSB_spj_Q50() throws Exception {
    TestUtil.test("dsb-sf1/parquet", "dsb/original/spj/q50_spj.sql", true);
  }

  @Test
  public void testDSB_spj_Q72() throws Exception {
    TestUtil.test("dsb-sf1/parquet", "dsb/original/spj/q72_spj.sql", true);
  }

  @Test
  public void testDSB_spj_Q84() throws Exception {
    TestUtil.test("dsb-sf1/parquet", "dsb/original/spj/q84_spj.sql", true);
  }

  @Test
  public void testDSB_spj_Q85() throws Exception {
    TestUtil.test("dsb-sf1/parquet", "dsb/original/spj/q85_spj.sql", true);
  }

  @Test
  public void testDSB_spj_Q91() throws Exception {
    TestUtil.test("dsb-sf1/parquet", "dsb/original/spj/q91_spj.sql", true);
  }

  @Test
  public void testDSB_spj_Q99() throws Exception {
    TestUtil.test("dsb-sf1/parquet", "dsb/original/spj/q99_spj.sql", true);
  }

  @Test
  public void testDSB_spj_Q100() throws Exception {
    TestUtil.test("dsb-sf1/parquet", "dsb/original/spj/q100_spj.sql", true);
  }

  @Test
  public void testDSB_spj_Q101() throws Exception {
    TestUtil.test("dsb-sf1/parquet", "dsb/original/spj/q101_spj.sql", true);
  }

  @Test
  public void testDSB_spj_Q102() throws Exception {
    TestUtil.test("dsb-sf1/parquet", "dsb/original/spj/q102_spj.sql", true);
  }

  @Test
  public void testDSB_mb_Q1() throws Exception {
    TestUtil.test("dsb-sf1/parquet", "dsb/original/multi-block/q1.sql", true);
  }

  @Test
  public void testDSB_mb_Q10() throws Exception {
    TestUtil.test("dsb-sf1/parquet", "dsb/original/multi-block/q10.sql", true);
  }

  @Test
  public void testDSB_mb_Q14() throws Exception {
    TestUtil.test("dsb-sf1/parquet", "dsb/original/multi-block/q14.sql", true);
  }

  @Test
  public void testDSB_mb_Q23() throws Exception {
    TestUtil.test("dsb-sf1/parquet", "dsb/original/multi-block/q23.sql", true);
  }

  @Test
  public void testDSB_mb_Q30() throws Exception {
    TestUtil.test("dsb-sf1/parquet", "dsb/original/multi-block/q30.sql", true);
  }

  @Test
  public void testDSB_mb_Q31() throws Exception {
    TestUtil.test("dsb-sf1/parquet", "dsb/original/multi-block/q31.sql", true);
  }

  @Test
  public void testDSB_mb_Q32() throws Exception {
    TestUtil.test("dsb-sf1/parquet", "dsb/original/multi-block/q32.sql", true);
  }

  @Test
  public void testDSB_mb_Q38() throws Exception {
    TestUtil.test("dsb-sf1/parquet", "dsb/original/multi-block/q38.sql", true);
  }

  @Test
  public void testDSB_mb_Q39_0() throws Exception {
    TestUtil.test("dsb-sf1/parquet", "dsb/original/multi-block/q39_0.sql", true);
  }

  @Test
  public void testDSB_mb_Q39_1() throws Exception {
    TestUtil.test("dsb-sf1/parquet", "dsb/original/multi-block/q39_1.sql", true);
  }

  @Test
  public void testDSB_mb_Q54() throws Exception {
    TestUtil.test("dsb-sf1/parquet", "dsb/original/multi-block/q54.sql", true);
  }

  @Test
  public void testDSB_mb_Q58() throws Exception {
    TestUtil.test("dsb-sf1/parquet", "dsb/original/multi-block/q58.sql", true);
  }

  @Test
  public void testDSB_mb_Q59() throws Exception {
    TestUtil.test("dsb-sf1/parquet", "dsb/original/multi-block/q59.sql", true);
  }

  @Test
  public void testDSB_mb_Q64() throws Exception {
    TestUtil.test("dsb-sf1/parquet", "dsb/original/multi-block/q64.sql", true);
  }

  @Test
  public void testDSB_mb_Q65() throws Exception {
    TestUtil.test("dsb-sf1/parquet", "dsb/original/multi-block/q65.sql", true);
  }

  @Test
  public void testDSB_mb_Q69() throws Exception {
    TestUtil.test("dsb-sf1/parquet", "dsb/original/multi-block/q69.sql", true);
  }

  @Test
  public void testDSB_mb_Q75() throws Exception {
    TestUtil.test("dsb-sf1/parquet", "dsb/original/multi-block/q75.sql", true);
  }

  @Test
  public void testDSB_mb_Q80() throws Exception {
    TestUtil.test("dsb-sf1/parquet", "dsb/original/multi-block/q80.sql", true);
  }

  @Test
  public void testDSB_mb_Q81() throws Exception {
    TestUtil.test("dsb-sf1/parquet", "dsb/original/multi-block/q81.sql", true);
  }

  @Test
  public void testDSB_mb_Q83() throws Exception {
    TestUtil.test("dsb-sf1/parquet", "dsb/original/multi-block/q83.sql", true);
  }

  @Test
  public void testDSB_mb_Q87() throws Exception {
    TestUtil.test("dsb-sf1/parquet", "dsb/original/multi-block/q87.sql", true);
  }

  @Test
  public void testDSB_mb_Q92() throws Exception {
    TestUtil.test("dsb-sf1/parquet", "dsb/original/multi-block/q92.sql", true);
  }

  @Test
  public void testDSB_mb_Q94() throws Exception {
    TestUtil.test("dsb-sf1/parquet", "dsb/original/multi-block/q94.sql", true);
  }

}
