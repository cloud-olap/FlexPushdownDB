package com.flexpushdowndb.calcite.schema;

import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;

import java.util.Map;

public final class SchemaImpl extends AbstractSchema {
  private final String schemaName;
  private final Map<String, Table> tableMap;
  private final Map<String, String> hashKeys;

  public SchemaImpl(String schemaName, Map<String, Table> tableMap, Map<String, String> hashKeys) {
    this.schemaName = schemaName;
    this.tableMap = tableMap;
    this.hashKeys = hashKeys;
  }

  public String getSchemaName() {
    return schemaName;
  }

  @Override
  public Map<String, Table> getTableMap() {
    return tableMap;
  }

  public Map<String, String> getHashKeys() {
    return hashKeys;
  }
}
