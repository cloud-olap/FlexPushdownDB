package com.flexpushdowndb.calcite.server;

import com.thrift.calciteserver.CalciteServer;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TServer.Args;
import org.apache.thrift.server.TSimpleServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import org.ini4j.Ini;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.nio.file.Paths;

public class Server {

  public void start() {
    try {
      // read conf
      InputStream is = getClass().getResourceAsStream("/config/exec.conf");
      Ini ini = new Ini(is);
      int serverPort = Integer.parseInt(ini.get("conf", "SERVER_PORT"));
      Path resourcePath = Paths.get(ini.get("conf", "RESOURCE_PATH"));

      // create and start thrift server
      CalciteServerHandler handler = new CalciteServerHandler(resourcePath);
      CalciteServer.Processor<CalciteServerHandler> processor = new CalciteServer.Processor<>(handler);
      TServerTransport serverTransport = new TServerSocket(serverPort);
      TServer server = new TSimpleServer(new Args(serverTransport).processor(processor));
      handler.setServerTransport(serverTransport);
      handler.setServer(server);
      System.out.println("[Java] Calcite server start...");
      server.serve();
    } catch (Exception e) {
      System.out.println("[Java] Calcite server start error");
      e.printStackTrace();
    }
  }

  public static void main(String[] args) throws IOException {
    Server server = new Server();
    server.start();
  }
}
