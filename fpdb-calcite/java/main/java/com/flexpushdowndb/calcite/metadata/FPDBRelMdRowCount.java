package com.flexpushdowndb.calcite.metadata;

import com.flexpushdowndb.calcite.optimizer.PushableHashJoinFinder;
import com.flexpushdowndb.calcite.rule.util.MoreRelOptUtil;
import com.flexpushdowndb.calcite.schema.TableImpl;
import org.apache.calcite.prepare.RelOptTableImpl;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.metadata.*;
import org.apache.calcite.util.BuiltInMethod;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;
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
    Double rowCount = null;

    // check if one side joins on key (unique values)
    List<Map.Entry<Integer, Integer>> joinKeys = MoreRelOptUtil.extractJoinKeys(rel.getCondition());
    if (joinKeys.size() == 1) {
      Map.Entry<Integer, Integer> joinKey = joinKeys.get(0);
      RelColumnOrigin leftColumnOrigin = mq.getColumnOrigin(rel, joinKey.getKey());
      RelColumnOrigin rightColumnOrigin = mq.getColumnOrigin(rel, joinKey.getValue());
      if (leftColumnOrigin != null && rightColumnOrigin != null &&
          !leftColumnOrigin.isDerived() && !rightColumnOrigin.isDerived()) {
        RelOptTableImpl leftTable = (RelOptTableImpl) leftColumnOrigin.getOriginTable();
        RelOptTableImpl rightTable = (RelOptTableImpl) rightColumnOrigin.getOriginTable();
        String leftJoinKey = leftTable.getRowType().getFieldNames().get(leftColumnOrigin.getOriginColumnOrdinal());
        String rightJoinKey = rightTable.getRowType().getFieldNames().get(rightColumnOrigin.getOriginColumnOrdinal());
        // FIXME: it may not be true that, if leftJoinKey's origin in base table is unique, then leftJoinKey is unique;
        //  leftJoinKey may not be unique if left rel is an intermediate join result
        if (((TableImpl)leftTable.table()).isUnique(leftJoinKey)) {
          rowCount = mq.getRowCount(rel.getRight());
        } else if (((TableImpl)rightTable.table()).isUnique(rightJoinKey)) {
          rowCount = mq.getRowCount(rel.getLeft());
        }
      }
    }

    // default estimation if no value is produced above
    if (rowCount == null) {
      rowCount = RelMdUtil.getJoinRowCount(mq, rel, rel.getCondition());
      if (rowCount == null) {
        return null;
      }
    }

    // adjust for co-located join pushdown
    return adjustJoinRowCountForPushdown(rel, rowCount);
  }

  private Double adjustJoinRowCountForPushdown(Join rel, double origRowCount) {
    if (THREAD_FIND_PUSHABLE_HASH_JOINS.get() &&
            PushableHashJoinFinder.isBottomHashJoin(rel) &&
            PushableHashJoinFinder.isJoinColocated(rel, THREAD_HASH_KEYS.get())) {
      return origRowCount / COLOCATE_JOIN_REDUCTION_FACTOR;
    } else {
      return origRowCount;
    }
  }
}
