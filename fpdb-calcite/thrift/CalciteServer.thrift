namespace java com.thrift.calciteserver

exception ParsePlanningError {
  1: string msg
}

struct TPlanResult {
  1: string plan_result
  2: i64 execution_time_ms
}

service CalciteServer {
  void ping(),
  void shutdown(),
  TPlanResult sql2Plan(1: string query, 2: string schemaName, 3: bool useHeuristicJoinOrdering)
      throws (1: ParsePlanningError parsePlanningErr),
  void updateMetadata(1: string catalog, 2: string table)
}