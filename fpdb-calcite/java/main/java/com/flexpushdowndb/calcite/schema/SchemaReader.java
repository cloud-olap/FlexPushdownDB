package com.flexpushdowndb.calcite.schema;

import com.flexpushdowndb.util.FileUtils;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.type.SqlTypeName;
import org.json.JSONObject;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

public class SchemaReader {

  public static SchemaImpl readSchema(Path resourcePath, String schemaName) throws Exception {
    Path schemaDirPath = resourcePath
            .resolve("metadata")
            .resolve(schemaName);

    // Read schema
    Path schemaPath = schemaDirPath.resolve("schema.json");
    Map<String, Map<String, SqlTypeName>> fieldTypesMap = readFieldTypes(schemaPath);

    // Read stats
    Path statsPath = schemaDirPath.resolve("stats.json");
    Map<String, Double> rowCounts = readRowCounts(statsPath);

    // Read hash keys if there is
    Path hashKeysPath = schemaDirPath.resolve("fpdb_store_hash_keys.json");
    Map<String, String> hashKeys = readHashKeys(hashKeysPath);

    // Construct tables
    Map<String, Table> tableMap = new HashMap<>();
    for (String tableName: fieldTypesMap.keySet()) {
      Map<String, SqlTypeName> fieldTypes = fieldTypesMap.get(tableName);
      double rowCount = rowCounts.get(tableName);
      tableMap.put(tableName, new TableImpl(tableName, fieldTypes, rowCount));
    }

    return new SchemaImpl(schemaName, tableMap, hashKeys);
  }

  private static Map<String, Map<String, SqlTypeName>> readFieldTypes(Path schemaPath) throws Exception {
    Map<String, Map<String, SqlTypeName>> fieldTypesMap = new HashMap<>();
    JSONObject jObj = new JSONObject(FileUtils.readFile(schemaPath));
    for (Object o: jObj.getJSONArray("tables")) {
      JSONObject schemaJObj = (JSONObject) o;
      String tableName = schemaJObj.getString("name");
      Map<String, SqlTypeName> fieldTypes = new HashMap<>();
      for (Object o1: schemaJObj.getJSONArray("fields")) {
        JSONObject fieldJObj = (JSONObject) o1;
        String fieldName = fieldJObj.getString("name");
        SqlTypeName fieldType = stringToSqlTypeName(fieldJObj.getString("type"));
        fieldTypes.put(fieldName, fieldType);
      }
      fieldTypesMap.put(tableName, fieldTypes);
    }
    return fieldTypesMap;
  }

  private static Map<String, Double> readRowCounts(Path statsPath) throws Exception {
    Map<String, Double> rowCounts = new HashMap<>();
    JSONObject jObj = new JSONObject(FileUtils.readFile(statsPath));
    for (Object o: jObj.getJSONArray("tables")) {
      JSONObject tableStatsJObj = (JSONObject) o;
      String tableName = tableStatsJObj.getString("name");
      JSONObject statsMapJObj = tableStatsJObj.getJSONObject("stats");
      double rowCount = statsMapJObj.getDouble("rowCount");
      rowCounts.put(tableName, rowCount);
    }
    return rowCounts;
  }

  private static Map<String, String> readHashKeys(Path hashKeysPath) throws Exception {
    if (!Files.exists(hashKeysPath)) {
      return new HashMap<>();
    }
    Map<String, String> hashKeys = new HashMap<>();
    JSONObject jObj = new JSONObject(FileUtils.readFile(hashKeysPath));
    for (Object o: jObj.getJSONArray("hashKeys")) {
      JSONObject hashKeyJObj = (JSONObject) o;
      String table = hashKeyJObj.getString("table");
      String key = hashKeyJObj.getString("key");
      hashKeys.put(table, key);
    }
    return hashKeys;
  }

  private static SqlTypeName stringToSqlTypeName(String typeString) {
    switch (typeString) {
      case "int32":
        return SqlTypeName.INTEGER;
      case "int64":
        return SqlTypeName.BIGINT;
      case "double":
        return SqlTypeName.DOUBLE;
      case "string":
        return SqlTypeName.VARCHAR;
      case "boolean":
        return SqlTypeName.BOOLEAN;
      case "date":
        return SqlTypeName.DATE;
      default:
        throw new UnsupportedOperationException("Unsupported field type: " + typeString);
    }
  }
}
