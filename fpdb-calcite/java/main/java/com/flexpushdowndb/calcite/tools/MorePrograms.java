package com.flexpushdowndb.calcite.tools;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.hep.HepMatchOrder;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rel.rules.JoinPushThroughJoinRule;
import org.apache.calcite.tools.Program;
import org.apache.calcite.tools.Programs;

import java.util.List;

public class MorePrograms {
  public static Program heuristicJoinOrder(
          final Iterable<? extends RelOptRule> rules,
          final boolean bushy, final int minJoinCount,
          RelMetadataProvider metadataProvider) {
    return (planner, rel, requiredOutputTraits, materializations, lattices) -> {
      final int joinCount = RelOptUtil.countJoins(rel);
      final Program program;
      if (joinCount < minJoinCount) {
        program = Programs.ofRules(rules);
      } else {
        // Create a program that gathers together joins as a MultiJoin.
        final HepProgram hep = new HepProgramBuilder()
                .addRuleInstance(CoreRules.FILTER_INTO_JOIN)
                .addMatchOrder(HepMatchOrder.BOTTOM_UP)
                .addRuleInstance(CoreRules.JOIN_TO_MULTI_JOIN)
                .build();
        final Program program1 =
                Programs.of(hep, false, metadataProvider);

        // Create a program that contains a rule to expand a MultiJoin
        // into heuristically ordered joins.
        // We use the rule set passed in, but remove JoinCommuteRule and
        // JoinPushThroughJoinRule, because they cause exhaustive search.
        final List<RelOptRule> list = Lists.newArrayList(rules);
        list.removeAll(
                ImmutableList.of(
                        CoreRules.JOIN_COMMUTE,
                        CoreRules.JOIN_ASSOCIATE,
                        JoinPushThroughJoinRule.LEFT,
                        JoinPushThroughJoinRule.RIGHT));
        list.add(bushy
                ? CoreRules.MULTI_JOIN_OPTIMIZE_BUSHY
                : CoreRules.MULTI_JOIN_OPTIMIZE);
        final Program program2 = Programs.ofRules(list);

        program = Programs.sequence(program1, program2);
      }
      return program.run(
              planner, rel, requiredOutputTraits, materializations, lattices);
    };
  }
}
