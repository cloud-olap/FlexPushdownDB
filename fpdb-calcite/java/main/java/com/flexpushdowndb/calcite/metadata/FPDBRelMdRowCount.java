package com.flexpushdowndb.calcite.metadata;

import com.flexpushdowndb.calcite.optimizer.PushableHashJoinFinder;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.metadata.*;
import org.apache.calcite.util.BuiltInMethod;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Map;

public class FPDBRelMdRowCount extends RelMdRowCount {
  public static final RelMetadataProvider SOURCE =
          ReflectiveRelMetadataProvider.reflectiveSource(BuiltInMethod.ROW_COUNT.method, new FPDBRelMdRowCount());

  // TODO: better to use RelDistribution instead of this
  public static final ThreadLocal<Map<String, String>> THREAD_HASH_KEYS = new ThreadLocal<>();
  public static final ThreadLocal<Boolean> THREAD_FIND_PUSHABLE_HASH_JOINS = new ThreadLocal<>();
  private static final Double COLOCATE_JOIN_REDUCTION_FACTOR = 2.0;

  @Override
  public @Nullable Double getRowCount(Join rel, RelMetadataQuery mq) {
    Double origRowCount = RelMdUtil.getJoinRowCount(mq, rel, rel.getCondition());
    if (origRowCount == null) {
      return null;
    }
    if (THREAD_FIND_PUSHABLE_HASH_JOINS.get() &&
            PushableHashJoinFinder.isBottomHashJoin(rel) &&
            PushableHashJoinFinder.isJoinColocated(rel, THREAD_HASH_KEYS.get())) {
      return origRowCount / COLOCATE_JOIN_REDUCTION_FACTOR;
    } else {
      return origRowCount;
    }
  }
}
