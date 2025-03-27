package com.flexpushdowndb.calcite.schema;

import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.util.ImmutableBitSet;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public final class TableImpl extends AbstractTable implements ScannableTable {
  private final String tableName;
  private final Map<String, FieldInfo> fields;
  private final double rowCount;
  private final List<String> orderedFieldNames;

  public TableImpl(String tableName, Map<String, FieldInfo> fields, double rowCount) {
    this.tableName = tableName;
    this.fields = fields;
    this.rowCount = rowCount;
    this.orderedFieldNames = new ArrayList<>(fields.keySet());
  }

  public String getTableName() {
    return tableName;
  }

  public boolean isUnique(String fieldName) {
    FieldInfo fieldInfo = fields.get(fieldName);
    if (fieldInfo == null) {
      return false;
    }
    return fieldInfo.isUnique();
  }

  @Override
  public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    RelDataTypeFactory.Builder typeBuilder = typeFactory.builder();
    for (String fieldName: orderedFieldNames) {
      FieldInfo fieldInfo = fields.get(fieldName);
      typeBuilder.add(fieldName, fieldInfo.getSqlTypeName())
              .nullable(fieldInfo.isNullable());
    }
    return typeBuilder.build();
  }

  @Override
  public Statistic getStatistic() {
    return new Statistic() {
      @Override
      public @Nullable Double getRowCount() {
        return rowCount;
      }

      @Override
      public boolean isKey(ImmutableBitSet columns) {
        // currently only support single-column keys
        for (int fieldId: columns) {
          if (fields.get(orderedFieldNames.get(fieldId)).isUnique()) {
            return true;
          }
        }
        return false;
      }
    };
  }

  @Override
  public Enumerable<Object[]> scan(DataContext root) {
    throw new UnsupportedOperationException("Not implemented");
  }
}
