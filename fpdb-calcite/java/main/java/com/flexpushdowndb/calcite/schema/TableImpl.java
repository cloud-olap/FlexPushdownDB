package com.flexpushdowndb.calcite.schema;

import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.Statistics;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.Map;

public final class TableImpl extends AbstractTable implements ScannableTable {
  private final String tableName;
  private final Map<String, SqlTypeName> fieldTypes;
  private final double rowCount;

  public TableImpl(String tableName, Map<String, SqlTypeName> fieldTypes, double rowCount) {
    this.tableName = tableName;
    this.fieldTypes = fieldTypes;
    this.rowCount = rowCount;
  }

  public String getTableName() {
    return tableName;
  }

  @Override
  public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    RelDataTypeFactory.Builder typeBuilder = typeFactory.builder();
    for (String fieldName: fieldTypes.keySet()) {
      typeBuilder.add(fieldName, fieldTypes.get(fieldName));
    }
    return typeBuilder.build();
  }

  @Override
  public Statistic getStatistic() {
    return Statistics.of(rowCount, null);
  }

  @Override
  public Enumerable<Object[]> scan(DataContext root) {
    throw new UnsupportedOperationException("Not implemented");
  }
}
