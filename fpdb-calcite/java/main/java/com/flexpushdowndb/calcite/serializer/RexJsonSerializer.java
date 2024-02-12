package com.flexpushdowndb.calcite.serializer;

import com.google.common.collect.BoundType;
import com.google.common.collect.Range;
import com.sun.org.apache.xpath.internal.operations.Bool;
import org.apache.calcite.avatica.SqlType;
import org.apache.calcite.avatica.util.TimeUnitRange;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.*;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.DateString;
import org.apache.calcite.util.NlsString;
import org.apache.calcite.util.Sarg;
import org.json.JSONArray;
import org.json.JSONObject;

import java.math.BigDecimal;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Set;

@SuppressWarnings({"BetaApi", "type.argument.type.incompatible", "UnstableApiUsage"})
public class RexJsonSerializer {
  public static JSONObject serialize(RexNode rexNode, List<String> fieldNames, RexBuilder rexBuilder) {
    return rexNode.accept(new RexVisitor<JSONObject>() {
      @Override
      public JSONObject visitInputRef(RexInputRef inputRef) {
        JSONObject jo = new JSONObject();
        jo.put("inputRef", fieldNames.get(inputRef.getIndex()));
        return jo;
      }

      @Override
      public JSONObject visitCall(RexCall call) {
        switch (call.getKind()) {
          case LESS_THAN:
          case GREATER_THAN:
          case LESS_THAN_OR_EQUAL:
          case GREATER_THAN_OR_EQUAL:
          case EQUALS:
          case NOT_EQUALS:
          case AND:
          case OR:
          case NOT:
          case PLUS:
          case MINUS:
          case TIMES:
          case DIVIDE:
          case LIKE:
          case EXTRACT:
          case IS_NULL: {
            JSONObject jo = new JSONObject();
            // op
            jo.put("op", call.getKind());
            // operands
            JSONArray operandsJArr = new JSONArray();
            for (RexNode operand : call.getOperands()) {
              operandsJArr.put(serialize(operand, fieldNames, rexBuilder));
            }
            jo.put("operands", operandsJArr);
            return jo;
          }
          case SEARCH: {
            RexInputRef searchRef = (RexInputRef) call.getOperands().get(0);
            String searchFieldName = fieldNames.get(searchRef.getIndex());
            RexLiteral literal = (RexLiteral) call.getOperands().get(1);
            return serializeSearch(searchFieldName, literal);
          }
          case CASE: {
            JSONObject jo = new JSONObject();
            // op
            jo.put("op", call.getKind());
            // condition
            jo.put("condition", visitCaseOperands(call.getOperands(), 0, fieldNames, rexBuilder));
            return jo;
          }
          case IS_TRUE:
          case IS_FALSE: {
            JSONObject jo = new JSONObject();
            // op
            jo.put("op", SqlKind.EQUALS);
            // operands
            JSONArray operandsJArr = new JSONArray();
            operandsJArr.put(serialize(call.getOperands().get(0), fieldNames, rexBuilder));
            if (call.getKind() == SqlKind.IS_TRUE) {
              operandsJArr.put(serialize(rexBuilder.makeLiteral(true), fieldNames, rexBuilder));
            } else {
              operandsJArr.put(serialize(rexBuilder.makeLiteral(false), fieldNames, rexBuilder));
            }
            jo.put("operands", operandsJArr);
            return jo;
          }
          case IS_NOT_TRUE:
          case IS_NOT_FALSE: {
            RexNode operand = call.getOperands().get(0);
            RexNode isNull = rexBuilder.makeCall(SqlStdOperatorTable.IS_NULL, operand);
            RexNode nodeIfNotNull = call.getKind() == SqlKind.IS_NOT_TRUE ?
                    rexBuilder.makeCall(SqlStdOperatorTable.IS_FALSE, operand):
                    rexBuilder.makeCall(SqlStdOperatorTable.IS_TRUE, operand);
            RexNode orNode = rexBuilder.makeCall(SqlStdOperatorTable.OR, isNull, nodeIfNotNull);
            return serialize(orNode, fieldNames, rexBuilder);
          }
          case OTHER_FUNCTION: {
            return visitOtherFunction(call, fieldNames);
          }
          case CAST: {
            RexNode operand = call.getOperands().get(0);
            RelDataType type = call.getType();
            JSONObject jo = new JSONObject();
            jo.put("op", call.op.getName());
            jo.put("operand", serialize(operand, fieldNames, rexBuilder));
            jo.put("type", type.getSqlTypeName());
            return jo;
          }
          default: {
            throw new UnsupportedOperationException("Serialize unsupported RexCall: " + call.getKind());
          }
        }
      }

      @Override
      public JSONObject visitLocalRef(RexLocalRef localRef) {
        throw new UnsupportedOperationException("Serialize unsupported RexNode: " + localRef.getClass().getSimpleName());
      }

      @Override
      public JSONObject visitLiteral(RexLiteral literal) {
        JSONObject literalJObj = new JSONObject();
        SqlTypeName basicTypeName = literal.getType().getSqlTypeName();
        Object type, value;

        switch (basicTypeName) {
          case CHAR:
          case VARCHAR: {
            type = basicTypeName;
            value = literal.getValueAs(String.class);
            break;
          }
          case INTEGER: {
            type = basicTypeName;
            value = literal.getValueAs(Integer.class);
            break;
          }
          case BIGINT: {
            type = basicTypeName;
            value = literal.getValueAs(Long.class);
            break;
          }
          case DOUBLE:
          case DECIMAL: {
            type = basicTypeName;
            value = literal.getValueAs(Double.class);
            break;
          }
          case BOOLEAN: {
            type = basicTypeName;
            value = literal.getValueAs(Boolean.class);
            break;
          }
          case DATE: {
            type = "DATE_MS";
            value = Objects.requireNonNull(literal.getValueAs(DateString.class)).getMillisSinceEpoch();
            break;
          }
          case INTERVAL_DAY: {
            type = "INTERVAL_DAY";
            value = literal.getValueAs(Long.class) / 86400_000;   // number of days
            break;
          }
          case INTERVAL_YEAR:
          case INTERVAL_MONTH:{
            type = "INTERVAL_MONTH";
            value = literal.getValueAs(Integer.class);            // number of months
            break;
          }
          case SYMBOL: {
            type = basicTypeName;
            value = literal.getValueAs(TimeUnitRange.class);
            break;
          }
          default:
            throw new UnsupportedOperationException("Serialize unsupported RexLiteral with basicTypeName: "
                    + basicTypeName);
        }

        literalJObj.put("type", type);
        if (value != null) {
          literalJObj.put("value", value);
        } else {
          literalJObj.put("value", JSONObject.NULL);
        }
        return new JSONObject().put("literal", literalJObj);
      }

      @Override
      public JSONObject visitOver(RexOver over) {
        throw new UnsupportedOperationException("Serialize unsupported RexNode: " + over.getClass().getSimpleName());
      }

      @Override
      public JSONObject visitCorrelVariable(RexCorrelVariable correlVariable) {
        throw new UnsupportedOperationException("Serialize unsupported RexNode: " + correlVariable.getClass().getSimpleName());
      }

      @Override
      public JSONObject visitDynamicParam(RexDynamicParam dynamicParam) {
        throw new UnsupportedOperationException("Serialize unsupported RexNode: " + dynamicParam.getClass().getSimpleName());
      }

      @Override
      public JSONObject visitRangeRef(RexRangeRef rangeRef) {
        throw new UnsupportedOperationException("Serialize unsupported RexNode: " + rangeRef.getClass().getSimpleName());
      }

      @Override
      public JSONObject visitFieldAccess(RexFieldAccess fieldAccess) {
        throw new UnsupportedOperationException("Serialize unsupported RexNode: " + fieldAccess.getClass().getSimpleName());
      }

      @Override
      public JSONObject visitSubQuery(RexSubQuery subQuery) {
        throw new UnsupportedOperationException("Serialize unsupported RexNode: " + subQuery.getClass().getSimpleName());
      }

      @Override
      public JSONObject visitTableInputRef(RexTableInputRef fieldRef) {
        throw new UnsupportedOperationException("Serialize unsupported RexNode: " + fieldRef.getClass().getSimpleName());
      }

      @Override
      public JSONObject visitPatternFieldRef(RexPatternFieldRef fieldRef) {
        throw new UnsupportedOperationException("Serialize unsupported RexNode: " + fieldRef.getClass().getSimpleName());
      }

      private JSONObject visitCaseOperands(List<RexNode> rexNodes,
                                           int startIndex,
                                           List<String> fieldNames,
                                           RexBuilder rexBuilder) {
        JSONObject jo = new JSONObject();
        jo.put("if", serialize(rexNodes.get(startIndex), fieldNames, rexBuilder));
        jo.put("then", serialize(rexNodes.get(startIndex + 1), fieldNames, rexBuilder));
        if (startIndex + 2 == rexNodes.size() - 1) {
          jo.put("else", serialize(rexNodes.get(startIndex + 2), fieldNames, rexBuilder));
        } else {
          jo.put("else", visitCaseOperands(rexNodes, startIndex + 2, fieldNames, rexBuilder));
        }
        return jo;
      }

      private JSONObject visitOtherFunction(RexCall call, List<String> fieldNames) {
        switch (call.op.getName()) {
          case "SUBSTRING": {
            JSONObject jo = new JSONObject();
            // op
            jo.put("op", call.op.getName());
            // operands
            JSONArray operandsJArr = new JSONArray();
            for (RexNode operand : call.getOperands()) {
              operandsJArr.put(serialize(operand, fieldNames, rexBuilder));
            }
            jo.put("operands", operandsJArr);
            return jo;
          }
          default: {
            throw new UnsupportedOperationException("Serialize unsupported OTHER_FUNCTION: " + call.op.getName());
          }
        }
      }
    });
  }

  private static JSONObject serializeSearch(String fieldName, RexLiteral literal) {
    Sarg<?> sarg = literal.getValueAs(Sarg.class);
    if (sarg == null) {
      throw new NullPointerException("Invalid Sarg: null");
    }

    // calibrate basicTypeName for Date
    SqlTypeName basicTypeName = literal.getType().getSqlTypeName();
    String basicTypeNameStr = basicTypeName.getName();
    if (basicTypeName == SqlTypeName.DATE) {
      basicTypeNameStr = "DATE_MS";
    }

    // inputRef
    JSONObject inputRefOperand = new JSONObject()
            .put("inputRef", fieldName);
    JSONObject rangeSetJObj = null;

    // check if it's "in"
    if (sarg.isPoints()) {
      return new JSONObject()
              .put("op", SqlKind.IN)
              .put("operands", new JSONArray()
                      .put(inputRefOperand)
                      .put(new JSONObject()
                              .put("literals", new JSONObject()
                                      .put("type", basicTypeNameStr)
                                      .put("values", serializeSargPoints(basicTypeName, sarg.rangeSet.asRanges())))));
    }

    // check if it's "not in"
    if (sarg.isComplementedPoints()) {
      JSONObject inJObj = new JSONObject()
              .put("op", SqlKind.IN)
              .put("operands", new JSONArray()
                      .put(inputRefOperand)
                      .put(new JSONObject()
                              .put("literals", new JSONObject()
                                      .put("type", basicTypeNameStr)
                                      .put("values", serializeSargComplementedPoints(basicTypeName, sarg.rangeSet.asRanges())))));
      return new JSONObject()
              .put("op", SqlKind.NOT)
              .put("operands", new JSONArray()
                      .put(inJObj));
    }

    // otherwise
    for (Object rangeObj: sarg.rangeSet.asRanges()) {
      Range<?> range = (Range<?>) rangeObj;
      JSONObject rangeJObj = new JSONObject();
      Comparable<?> lowerValue = range.hasLowerBound() ?
              serializeSargEndPoint(basicTypeName, range.lowerEndpoint()) : null;
      Comparable<?> upperValue = range.hasUpperBound() ?
              serializeSargEndPoint(basicTypeName, range.upperEndpoint()) : null;

      if (lowerValue != null && lowerValue.equals(upperValue)) {
        // lower value = upper value
        JSONObject literalOperand = new JSONObject()
                .put("literal", new JSONObject()
                        .put("type", basicTypeNameStr)
                        .put("value", lowerValue));
        rangeJObj.put("op", SqlKind.EQUALS)
                .put("operands", new JSONArray()
                        .put(inputRefOperand)
                        .put(literalOperand));
      } else {
        // lower value != upper value
        // lower
        SqlKind lowerOp = range.lowerBoundType() == BoundType.OPEN ?
                SqlKind.GREATER_THAN : SqlKind.GREATER_THAN_OR_EQUAL;
        JSONObject lowerLiteralOperand = new JSONObject()
                .put("literal", new JSONObject()
                        .put("type", basicTypeNameStr)
                        .put("value", lowerValue));
        JSONObject rangeLowerJObj = new JSONObject()
                .put("op", lowerOp)
                .put("operands", new JSONArray()
                        .put(inputRefOperand)
                        .put(lowerLiteralOperand));

        // upper
        SqlKind upperOp = range.upperBoundType() == BoundType.OPEN ?
                SqlKind.LESS_THAN : SqlKind.LESS_THAN_OR_EQUAL;
        JSONObject upperLiteralOperand = new JSONObject()
                .put("literal", new JSONObject()
                        .put("type", basicTypeNameStr)
                        .put("value", upperValue));
        JSONObject rangeUpperJObj = new JSONObject()
                .put("op", upperOp)
                .put("operands", new JSONArray()
                        .put(inputRefOperand)
                        .put(upperLiteralOperand));

        // make lower and upper an And
        rangeJObj.put("op", SqlKind.AND)
                .put("operands", new JSONArray()
                        .put(rangeLowerJObj)
                        .put(rangeUpperJObj));
      }

      // if more than one range, make them an Or
      if (rangeSetJObj == null) {
        rangeSetJObj = rangeJObj;
      } else {
        rangeSetJObj = new JSONObject()
                .put("op", SqlKind.OR)
                .put("operands", new JSONArray()
                        .put(rangeSetJObj)
                        .put(rangeJObj));
      }
    }

    return rangeSetJObj;
  }

  private static JSONArray serializeSargPoints(SqlTypeName basicTypeName, Set<? extends Range<?>> ranges) {
    JSONArray jArr = new JSONArray();
    for (Range<?> range: ranges) {
      jArr.put(serializeSargEndPoint(basicTypeName, range.lowerEndpoint()));
    }
    return jArr;
  }

  private static JSONArray serializeSargComplementedPoints(SqlTypeName basicTypeName, Set<? extends Range<?>> ranges) {
    JSONArray jArr = new JSONArray();
    for (Range<?> range: ranges) {
      if (range.hasLowerBound()) {
        jArr.put(serializeSargEndPoint(basicTypeName, range.lowerEndpoint()));
      }
    }
    return jArr;
  }

  private static Comparable<?> serializeSargEndPoint(SqlTypeName basicTypeName, Comparable<?> endpoint) {
    switch (basicTypeName) {
      case CHAR:
      case VARCHAR: {
        return ((NlsString) endpoint).getValue();
      }
      case INTEGER: {
        return ((BigDecimal) endpoint).intValue();
      }
      case BIGINT: {
        return ((BigDecimal) endpoint).longValue();
      }
      case DOUBLE:
      case DECIMAL: {
        return ((BigDecimal) endpoint).doubleValue();
      }
      case DATE: {
        return ((DateString) endpoint).getMillisSinceEpoch();
      }
      default: {
        throw new UnsupportedOperationException("Serialize unsupported RexLiteral with basicTypeName: "
                + basicTypeName);
      }
    }
  }
}
