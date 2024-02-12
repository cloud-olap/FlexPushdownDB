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
package com.flexpushdowndb.calcite.rule;

import com.flexpushdowndb.calcite.rule.util.FilterJoinRulesUtil;
import com.flexpushdowndb.calcite.rule.util.MoreRelOptUtil;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilder;

import java.util.List;

/**
 * Rule that pushes predicates from a Filter and a Join into the Join below, and pushes the join
 * condition into the Join.
 */
public abstract class EnhancedFilterJoinRule extends RelOptRule {
  public static final EnhancedFilterJoinRule WITH_FILTER = new WithFilter();
  public static final EnhancedFilterJoinRule NO_FILTER = new NoFilter();

  private EnhancedFilterJoinRule(RelOptRuleOperand operand, String desc) {
    super(operand, "FilterJoinRule:" + desc);
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    Filter filterRel;
    Join joinRel;
    if (call.rels.length == 1) {
      filterRel = null;
      joinRel = call.rel(0);
    } else {
      filterRel = call.rel(0);
      joinRel = call.rel(1);
    }
    RelNode rewrite = doMatch(filterRel, joinRel, call.builder());
    if (rewrite != null) {
      call.transformTo(rewrite);
    }
  }

  protected RelNode doMatch(Filter filterRel, Join joinRel, RelBuilder relBuilder) {
    // Extract the join condition and pushdown predicates, also simplify the remaining filter
    EnhancedFilterJoinExtraction extraction = new EnhancedFilterJoinExtractor(filterRel, joinRel,
            FilterJoinRulesUtil.EQUAL_IS_NOT_DISTINCT_FROM).extract();
    if (extraction == null) {
      return null;
    }

    RexNode inputFilterConditionPruned = extraction.getInputFilterConditionPruned();
    RexNode inputJoinConditionPruned = extraction.getInputJoinConditionPruned();
    RexNode newJoinCondition = extraction.getJoinCondition();
    RexNode leftPushdownPredicate = extraction.getLeftPushdownPredicate();
    RexNode rightPushdownPredicate = extraction.getRightPushdownPredicate();
    RexNode remainingFilterCondition = extraction.getRemainingFilterCondition();
    JoinRelType simplifiedJoinType = extraction.getSimplifiedJoinType();

    // Shift filters
    List<RelDataTypeField> joinFields = joinRel.getRowType().getFieldList();
    List<RelDataTypeField> leftFields = joinRel.getInputs().get(0).getRowType().getFieldList();
    List<RelDataTypeField> rightFields = joinRel.getInputs().get(1).getRowType().getFieldList();
    RexBuilder rexBuilder = joinRel.getCluster().getRexBuilder();
    RexNode leftPushdownPredicateShifted = MoreRelOptUtil.shiftFilter(
            0,
            leftFields.size(),
            0,
            rexBuilder,
            joinFields,
            joinFields.size(),
            leftFields,
            leftPushdownPredicate);
    RexNode rightPushdownPredicateShifted = MoreRelOptUtil.shiftFilter(
            leftFields.size(),
            joinFields.size(),
            -leftFields.size(),
            rexBuilder,
            joinFields,
            joinFields.size(),
            rightFields,
            rightPushdownPredicate);

    // Prune left and right predicates that have already been pushed down
    RelMetadataQuery mq = joinRel.getCluster().getMetadataQuery();
    RexNode leftPushdownPredicatePruned = EnhancedFilterJoinPruner.prunePushdown(
            leftPushdownPredicateShifted, mq, joinRel.getLeft(), rexBuilder);
    RexNode rightPushdownPredicatePruned = EnhancedFilterJoinPruner.prunePushdown(
            rightPushdownPredicateShifted, mq, joinRel.getRight(), rexBuilder);

    // If nothing is changed, then no pushdown happens
    if (leftPushdownPredicatePruned.isAlwaysTrue()
            && rightPushdownPredicatePruned.isAlwaysTrue()
            && (newJoinCondition.isAlwaysTrue() ||
            (newJoinCondition.equals(inputJoinConditionPruned)
                    && remainingFilterCondition.equals(inputFilterConditionPruned)))) {
      return null;
    }

    // Construct the rewritten result
    return relBuilder
            .push(joinRel.getLeft())
            .filter(leftPushdownPredicatePruned)   // left child of join
            .push(joinRel.getRight())
            .filter(rightPushdownPredicatePruned)    // right child of join
            .join(simplifiedJoinType, newJoinCondition)    // join
            .convert(joinRel.getRowType(), false)   // project if needed
            .filter(remainingFilterCondition)   // remaining filter
            .build();
  }

  private static class WithFilter extends EnhancedFilterJoinRule {
    private WithFilter() {
      super(RelOptRule.operand(Filter.class,
              RelOptRule.operand(Join.class, RelOptRule.any())), "EnhancedFilterJoinRule:filter");
    }
  }

  private static class NoFilter extends EnhancedFilterJoinRule {
    private NoFilter() {
      super(RelOptRule.operand(Join.class, RelOptRule.any()),
              "EnhancedFilterJoinRule:no-filter");
    }
  }
}