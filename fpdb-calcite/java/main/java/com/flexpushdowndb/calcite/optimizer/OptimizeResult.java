package com.flexpushdowndb.calcite.optimizer;

import org.apache.calcite.adapter.enumerable.EnumerableHashJoin;
import org.apache.calcite.rel.RelNode;

import java.util.Set;

public class OptimizeResult {
  private final RelNode plan;
  private final Set<EnumerableHashJoin> pushableHashJoins;

  public OptimizeResult(RelNode plan, Set<EnumerableHashJoin> pushableHashJoins) {
    this.plan = plan;
    this.pushableHashJoins = pushableHashJoins;
  }

  public RelNode getPlan() {
    return plan;
  }

  public Set<EnumerableHashJoin> getPushableHashJoins() {
    return pushableHashJoins;
  }
}
