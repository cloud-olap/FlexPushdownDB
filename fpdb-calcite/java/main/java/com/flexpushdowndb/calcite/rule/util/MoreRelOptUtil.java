/*
 * Copyright (C) 2017-2019 Dremio Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.flexpushdowndb.calcite.rule.util;

import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlKind;

import java.util.List;

public final class MoreRelOptUtil {

  /**
   * Same as {@code RelOptUtil.shiftFilter()}.
   * Shift filters using inputs ref of joins to filters using input refs of children of joins
   */
  public static RexNode shiftFilter(
          int start,
          int end,
          int offset,
          RexBuilder rexBuilder,
          List<RelDataTypeField> joinFields,
          int nTotalFields,
          List<RelDataTypeField> rightFields,
          RexNode filter) {
    int[] adjustments = new int[nTotalFields];
    for (int i = start; i < end; i++) {
      adjustments[i] = offset;
    }
    return filter.accept(
            new RelOptUtil.RexInputConverter(
                    rexBuilder,
                    joinFields,
                    rightFields,
                    adjustments));
  }

  /**
   * Combination of {@link RelOptUtil#conjunctions(RexNode)} and {@link RelOptUtil#disjunctions(RexNode)}.
   * Check {@code RexNode.getKind()} and for AND/OR return unnested elements.
   */
  public static List<RexNode> conDisjunctions(RexNode rexNode) {
    switch (rexNode.getKind()) {
      case AND:
        return RelOptUtil.conjunctions(rexNode);
      case OR:
        return RelOptUtil.disjunctions(rexNode);
      default:
        throw new UnsupportedOperationException();
    }
  }

  /**
   * Combination of {@link RexUtil#composeConjunction(RexBuilder, Iterable, boolean)} and
   * {@link RexUtil#composeDisjunction(RexBuilder, Iterable, boolean)}.
   * Check {@code RexNode.getKind()} and for AND/OR compose expressions using {@code nodes}.
   */
  public static RexNode composeConDisjunction(RexBuilder rexBuilder,
                                              Iterable<? extends RexNode> nodes, boolean nullOnEmpty, SqlKind sqlKind) {
    switch (sqlKind) {
      case AND:
        return RexUtil.composeConjunction(rexBuilder, nodes, nullOnEmpty);
      case OR:
        return RexUtil.composeDisjunction(rexBuilder, nodes, nullOnEmpty);
      default:
        throw new UnsupportedOperationException();
    }
  }
}
