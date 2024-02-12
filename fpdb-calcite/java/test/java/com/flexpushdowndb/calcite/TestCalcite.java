package com.flexpushdowndb.calcite;

import com.flexpushdowndb.calcite.rule.JoinSmallLeftRule;
import com.flexpushdowndb.calcite.schema.SchemaImpl;
import com.flexpushdowndb.calcite.schema.TableImpl;
import com.google.common.collect.ImmutableMap;
import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.enumerable.EnumerableRules;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.*;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.SqlExplainFormat;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.StandardConvertletTable;
import org.apache.calcite.tools.Program;
import org.apache.calcite.tools.Programs;
import org.apache.calcite.tools.RuleSet;
import org.apache.calcite.tools.RuleSets;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

public class TestCalcite {
  private static final RelOptTable.ViewExpander NOOP_EXPANDER = (type, query, schema, path) -> null;

  private static RelOptCluster newCluster(RelDataTypeFactory typeFactory) {
    RelOptPlanner planner = new VolcanoPlanner(RelOptCostImpl.FACTORY, Contexts.of(CalciteConnectionConfig.DEFAULT));
    planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
    return RelOptCluster.create(planner, new RexBuilder(typeFactory));
  }

  public static void main(String[] args) throws Exception {
    String sqlQuery = "SELECT T1.A FROM T5, T2, T3, T4, T1\n" +
            "WHERE T1.A = T2.A " +
            "AND T3.B = T2.B " +
            "AND T4.C = T2.C " +
            "AND T5.D = T2.D " +
            "AND T3.E = 333";
//    String sqlQuery = "SELECT T2.E FROM T1, T2, T3 " +
//            "WHERE T1.A = T2.A " +
//            "AND T3.B = T2.B";

    // Prepare typeFactory
    RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();

    // Prepare the schema
    TableImpl t1 = new TableImpl("T1",
            ImmutableMap.of(
                    "A", SqlTypeName.INTEGER,
                    "B", SqlTypeName.INTEGER,
                    "C", SqlTypeName.INTEGER,
                    "D", SqlTypeName.INTEGER,
                    "E", SqlTypeName.INTEGER),
            88888);
    TableImpl t2 = new TableImpl("T2",
            ImmutableMap.of(
                    "A", SqlTypeName.INTEGER,
                    "B", SqlTypeName.INTEGER,
                    "C", SqlTypeName.INTEGER,
                    "D", SqlTypeName.INTEGER,
                    "E", SqlTypeName.INTEGER),
            9999999);
    TableImpl t3 = new TableImpl("T3",
            ImmutableMap.of(
                    "A", SqlTypeName.INTEGER,
                    "B", SqlTypeName.INTEGER,
                    "C", SqlTypeName.INTEGER,
                    "D", SqlTypeName.INTEGER,
                    "E", SqlTypeName.INTEGER),
            7777);
    TableImpl t4 = new TableImpl("T4",
            ImmutableMap.of(
                    "A", SqlTypeName.INTEGER,
                    "B", SqlTypeName.INTEGER,
                    "C", SqlTypeName.INTEGER,
                    "D", SqlTypeName.INTEGER,
                    "E", SqlTypeName.INTEGER),
            666);
    TableImpl t5 = new TableImpl("T5",
            ImmutableMap.of(
                    "A", SqlTypeName.INTEGER,
                    "B", SqlTypeName.INTEGER,
                    "C", SqlTypeName.INTEGER,
                    "D", SqlTypeName.INTEGER,
                    "E", SqlTypeName.INTEGER),
            100);
    CalciteSchema rootSchema = CalciteSchema.createRootSchema(false, true);
    SchemaImpl schema1 = new SchemaImpl("a", ImmutableMap.of(), new HashMap<>());
    rootSchema.add(schema1.getSchemaName(), schema1);
    SchemaImpl schema = new SchemaImpl(
            "MYSCHEMA",
            ImmutableMap.of(
                    t1.getTableName(), t1,
                    t2.getTableName(), t2,
                    t3.getTableName(), t3,
                    t4.getTableName(), t4,
                    t5.getTableName(), t5),
            new HashMap<>());
    rootSchema.add(schema.getSchemaName(), schema);

    // Create an SQL parser and parse the query into AST
    SqlParser parser = SqlParser.create(sqlQuery);
    SqlNode parseAst = parser.parseQuery();

    // Configure and instantiate the catalog reader
    CalciteCatalogReader catalogReader = new CalciteCatalogReader(rootSchema, Collections.singletonList(schema.getSchemaName()),
            typeFactory, CalciteConnectionConfig.DEFAULT.set(CalciteConnectionProperty.CASE_SENSITIVE, "false"));

    // Create the SQL validator using the standard operator table and default configuration,
    // then validate the initial AST
    SqlValidator validator = SqlValidatorUtil.newValidator(SqlStdOperatorTable.instance(), catalogReader,
            typeFactory, SqlValidator.Config.DEFAULT);
    SqlNode validAst = validator.validate(parseAst);

    // Create the optimization cluster to maintain planning information,
    // then convert the AST to a logical plan
    RelOptCluster cluster = newCluster(typeFactory);
    SqlToRelConverter sqlToRelConverter = new SqlToRelConverter(NOOP_EXPANDER,
            validator,
            catalogReader,
            cluster,
            StandardConvertletTable.INSTANCE,
            SqlToRelConverter.config());
    RelNode logicalPlan = sqlToRelConverter.convertQuery(validAst, false, true).rel;
    System.out.println(RelOptUtil.dumpPlan("[Logical plan]", logicalPlan, SqlExplainFormat.TEXT,
            SqlExplainLevel.ALL_ATTRIBUTES));

    RelOptPlanner planner = cluster.getPlanner();

    List<RelOptRule> ruleList = new ArrayList<>();
    for (RelOptRule rule: Programs.RULE_SET) {
      if (!rule.equals(EnumerableRules.ENUMERABLE_MERGE_JOIN_RULE)) {
        ruleList.add(rule);
      }
    }
    RuleSet rules = RuleSets.ofList(ruleList);

    Program program = Programs.of(RuleSets.ofList(rules));
    RelNode res = program.run(
            planner,
            logicalPlan,
            logicalPlan.getTraitSet().plus(EnumerableConvention.INSTANCE),
            Collections.emptyList(),
            Collections.emptyList()
    );
    System.out.println(RelOptUtil.dumpPlan("[Physical plan]", res, SqlExplainFormat.TEXT,
            SqlExplainLevel.ALL_ATTRIBUTES));

    HepProgram hepProgram = new HepProgramBuilder()
            .addRuleInstance(JoinSmallLeftRule.INSTANCE)
            .addRuleInstance(EnumerableRules.ENUMERABLE_PROJECT_RULE)
            .build();
    HepPlanner hepPlanner = new HepPlanner(hepProgram);
    hepPlanner.setRoot(res);
    res = hepPlanner.findBestExp();
    System.out.println(RelOptUtil.dumpPlan("[Physical plan2]", res, SqlExplainFormat.TEXT,
            SqlExplainLevel.ALL_ATTRIBUTES));
  }
}
