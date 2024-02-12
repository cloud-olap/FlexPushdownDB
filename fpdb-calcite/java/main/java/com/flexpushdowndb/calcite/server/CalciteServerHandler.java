package com.flexpushdowndb.calcite.server;

import com.flexpushdowndb.calcite.optimizer.OptimizeResult;
import com.flexpushdowndb.calcite.optimizer.Optimizer;
import com.flexpushdowndb.calcite.serializer.RelJsonSerializer;
import com.thrift.calciteserver.CalciteServer;
import com.thrift.calciteserver.ParsePlanningError;
import com.thrift.calciteserver.TPlanResult;
import org.apache.thrift.TException;
import org.apache.thrift.server.TServer;
import org.apache.thrift.transport.TServerTransport;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.Path;

public class CalciteServerHandler implements CalciteServer.Iface{
  private final Optimizer optimizer;
  private TServerTransport serverTransport;
  private TServer server;

  public CalciteServerHandler(Path resourcePath) throws Exception {
    this.optimizer = new Optimizer(resourcePath);
  }

  public void setServerTransport(TServerTransport serverTransport) {
    this.serverTransport = serverTransport;
  }

  public void setServer(TServer server) {
    this.server = server;
  }

  @Override
  public void ping() {
    System.out.println("[Java] Client ping");
  }

  @Override
  public void shutdown() {
    server.stop();
    serverTransport.close();
    System.out.println("[Java] Calcite server shutdown...");
  }

  @Override
  public TPlanResult sql2Plan(String query, String schemaName, boolean useHeuristicJoinOrdering) throws TException {
    long startTime = System.currentTimeMillis();
    TPlanResult tPlanResult = new TPlanResult();
    try {
      OptimizeResult optimizeResult = optimizer.planQuery(query, schemaName, useHeuristicJoinOrdering);
      tPlanResult.plan_result = RelJsonSerializer
              .serialize(optimizeResult.getPlan(), optimizeResult.getPushableHashJoins())
              .toString(2);
    } catch (Exception e) {
      StringWriter sw = new StringWriter();
      PrintWriter pw = new PrintWriter(sw);
      e.printStackTrace(pw);
      throw new ParsePlanningError(sw.toString());
    }
    tPlanResult.execution_time_ms = System.currentTimeMillis() - startTime;
    return tPlanResult;
  }

  @Override
  public void updateMetadata(String catalog, String table) {}
}
