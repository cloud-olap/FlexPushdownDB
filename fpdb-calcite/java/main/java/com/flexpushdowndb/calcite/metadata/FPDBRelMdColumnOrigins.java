package com.flexpushdowndb.calcite.metadata;

import com.google.common.base.MoreObjects;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.*;
import org.apache.calcite.rel.metadata.*;
import org.apache.calcite.util.BuiltInMethod;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Set;

public class FPDBRelMdColumnOrigins implements MetadataHandler<BuiltInMetadata.ColumnOrigin> {
  public static final RelMetadataProvider SOURCE = ReflectiveRelMetadataProvider.reflectiveSource(
          BuiltInMethod.COLUMN_ORIGIN.method, new FPDBRelMdColumnOrigins());
  private static final RelMdColumnOrigins BUILTIN_HANDLER = (RelMdColumnOrigins)
          RelMdColumnOrigins.SOURCE.handlers(BuiltInMetadata.ColumnOrigin.DEF).values().iterator().next();

  @Override
  public MetadataDef<BuiltInMetadata.ColumnOrigin> getDef() {
    return BuiltInMetadata.ColumnOrigin.DEF;
  }

  public Set<RelColumnOrigin> getColumnOrigins(RelSubset rel, RelMetadataQuery mq, int iOutputColumn) {
    return mq.getColumnOrigins(MoreObjects.firstNonNull(rel.getBest(), rel.getOriginal()), iOutputColumn);
  }

  // The following just used built-in implementation
  public @Nullable Set<RelColumnOrigin> getColumnOrigins(Aggregate rel,
                                                         RelMetadataQuery mq, int iOutputColumn) {
    return BUILTIN_HANDLER.getColumnOrigins(rel, mq, iOutputColumn);
  }

  public @Nullable Set<RelColumnOrigin> getColumnOrigins(Join rel, RelMetadataQuery mq,
                                                         int iOutputColumn) {
    return BUILTIN_HANDLER.getColumnOrigins(rel, mq, iOutputColumn);
  }

  public @Nullable Set<RelColumnOrigin> getColumnOrigins(SetOp rel,
                                                         RelMetadataQuery mq, int iOutputColumn) {
    return BUILTIN_HANDLER.getColumnOrigins(rel, mq, iOutputColumn);
  }

  public @Nullable Set<RelColumnOrigin> getColumnOrigins(Project rel,
                                                         final RelMetadataQuery mq, int iOutputColumn) {
    return BUILTIN_HANDLER.getColumnOrigins(rel, mq, iOutputColumn);
  }

  public @Nullable Set<RelColumnOrigin> getColumnOrigins(Calc rel,
                                                         final RelMetadataQuery mq, int iOutputColumn) {
    return BUILTIN_HANDLER.getColumnOrigins(rel, mq, iOutputColumn);
  }

  public @Nullable Set<RelColumnOrigin> getColumnOrigins(Filter rel,
                                                         RelMetadataQuery mq, int iOutputColumn) {
    return BUILTIN_HANDLER.getColumnOrigins(rel, mq, iOutputColumn);
  }

  public @Nullable Set<RelColumnOrigin> getColumnOrigins(Sort rel, RelMetadataQuery mq,
                                                         int iOutputColumn) {
    return BUILTIN_HANDLER.getColumnOrigins(rel, mq, iOutputColumn);
  }

  public @Nullable Set<RelColumnOrigin> getColumnOrigins(TableModify rel, RelMetadataQuery mq,
                                                         int iOutputColumn) {
    return BUILTIN_HANDLER.getColumnOrigins(rel, mq, iOutputColumn);
  }

  public @Nullable Set<RelColumnOrigin> getColumnOrigins(Exchange rel,
                                                         RelMetadataQuery mq, int iOutputColumn) {
    return BUILTIN_HANDLER.getColumnOrigins(rel, mq, iOutputColumn);
  }

  public @Nullable Set<RelColumnOrigin> getColumnOrigins(TableFunctionScan rel,
                                                         RelMetadataQuery mq, int iOutputColumn) {
    return BUILTIN_HANDLER.getColumnOrigins(rel, mq, iOutputColumn);
  }

  // Catch-all rule when none of the others apply.
  public @Nullable Set<RelColumnOrigin> getColumnOrigins(RelNode rel,
                                                         RelMetadataQuery mq, int iOutputColumn) {
    return BUILTIN_HANDLER.getColumnOrigins(rel, mq, iOutputColumn);
  }
}
