package com.flexpushdowndb.calcite;

import com.flexpushdowndb.calcite.optimizer.OptimizeResult;
import com.flexpushdowndb.calcite.optimizer.Optimizer;
import com.flexpushdowndb.calcite.serializer.RelJsonSerializer;
import com.flexpushdowndb.util.FileUtils;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.sql.SqlExplainFormat;
import org.apache.calcite.sql.SqlExplainLevel;
import org.ini4j.Ini;

import java.io.InputStream;
import java.nio.file.Path;
import java.nio.file.Paths;

public class TestUtil {

  public static void test(String schemaName, String queryFileName, boolean showJsonPlan) throws Exception {
    new TestUtil().testOne(schemaName, queryFileName, showJsonPlan, true);
  }

  public static void testNoHeuristicJoinOrdering(String schemaName, String queryFileName, boolean showJsonPlan)
          throws Exception {
    new TestUtil().testOne(schemaName, queryFileName, showJsonPlan, false);
  }

  public void testOne(String schemaName, String queryFileName, boolean showJsonPlan, boolean useHeuristicJoinOrdering)
          throws Exception {
    // resource path
    InputStream is = getClass().getResourceAsStream("/config/exec.conf");
    Ini ini = new Ini(is);
    Path resourcePath = Paths.get(ini.get("conf", "RESOURCE_PATH"));

    // read query
    Path queryPath = resourcePath
            .resolve("query")
            .resolve(queryFileName);
    String query = FileUtils.readFile(queryPath);

    // plan
    Optimizer optimizer = new Optimizer(resourcePath);
    OptimizeResult optimizeResult = optimizer.planQuery(query, schemaName, useHeuristicJoinOrdering);
    System.out.println(RelOptUtil.dumpPlan("[Optimized plan]", optimizeResult.getPlan(), SqlExplainFormat.TEXT,
            SqlExplainLevel.ALL_ATTRIBUTES));
    if (showJsonPlan) {
      System.out.println("[Serialized json plan]\n" + RelJsonSerializer
              .serialize(optimizeResult.getPlan(), optimizeResult.getPushableHashJoins())
              .toString(2));
    }
  }
}
