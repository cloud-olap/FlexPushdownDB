package com.flexpushdowndb.calcite.schema;

import org.apache.calcite.sql.type.SqlTypeName;

public final class FieldInfo {
  private final SqlTypeName sqlTypeName;
  private final boolean nullable;
  private final boolean unique;

  public FieldInfo(SqlTypeName sqlTypeName, boolean nullable, boolean unique) {
    this.sqlTypeName = sqlTypeName;
    this.nullable = nullable;
    this.unique = unique;
  }

  public SqlTypeName getSqlTypeName() {
    return sqlTypeName;
  }

  public boolean isNullable() {
    return nullable;
  }

  public boolean isUnique() {
    return unique;
  }
}
