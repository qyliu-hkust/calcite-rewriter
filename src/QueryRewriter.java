import org.apache.calcite.config.Lex;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.hep.HepMatchOrder;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.rel2sql.RelToSqlConverter;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rel.rules.PruneEmptyRules;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.dialect.PostgresqlSqlDialect;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.tools.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.apache.calcite.avatica.util.Casing.UNCHANGED;
import static org.apache.calcite.avatica.util.Quoting.DOUBLE_QUOTE;

public class QueryRewriter {
    private final RelToSqlConverter relToSqlConverter;
    private final HepPlanner hepPlanner;
    private final Planner planner;
    private final SqlDialect sqlDialect = PostgresqlSqlDialect.DEFAULT;


    private final Map<String, List<RelOptRule>> ruleMap = Map.of(
            "rule_agg", List.of(CoreRules.AGGREGATE_EXPAND_DISTINCT_AGGREGATES,CoreRules.AGGREGATE_EXPAND_DISTINCT_AGGREGATES_TO_JOIN,CoreRules.AGGREGATE_JOIN_TRANSPOSE_EXTENDED,CoreRules.AGGREGATE_PROJECT_MERGE,CoreRules.AGGREGATE_ANY_PULL_UP_CONSTANTS,CoreRules.AGGREGATE_UNION_AGGREGATE,CoreRules.AGGREGATE_UNION_TRANSPOSE,CoreRules.AGGREGATE_VALUES, PruneEmptyRules.AGGREGATE_INSTANCE),
            "rule_filter", List.of(CoreRules.FILTER_AGGREGATE_TRANSPOSE,CoreRules.FILTER_CORRELATE,CoreRules.FILTER_INTO_JOIN,CoreRules.JOIN_CONDITION_PUSH,CoreRules.FILTER_MERGE,CoreRules.FILTER_MULTI_JOIN_MERGE, CoreRules.FILTER_PROJECT_TRANSPOSE,CoreRules.FILTER_SET_OP_TRANSPOSE,CoreRules.FILTER_TABLE_FUNCTION_TRANSPOSE,CoreRules.FILTER_SCAN,CoreRules.FILTER_REDUCE_EXPRESSIONS,CoreRules.PROJECT_REDUCE_EXPRESSIONS,PruneEmptyRules.FILTER_INSTANCE),
            "rule_join",List.of(CoreRules.JOIN_EXTRACT_FILTER,CoreRules.JOIN_PROJECT_BOTH_TRANSPOSE,CoreRules.JOIN_PROJECT_LEFT_TRANSPOSE,CoreRules.JOIN_PROJECT_RIGHT_TRANSPOSE,CoreRules.JOIN_LEFT_UNION_TRANSPOSE,CoreRules.JOIN_RIGHT_UNION_TRANSPOSE,CoreRules.SEMI_JOIN_REMOVE,CoreRules.JOIN_REDUCE_EXPRESSIONS,PruneEmptyRules.JOIN_LEFT_INSTANCE,PruneEmptyRules.JOIN_RIGHT_INSTANCE),
            "rule_project", List.of(CoreRules.PROJECT_CALC_MERGE,CoreRules.PROJECT_CORRELATE_TRANSPOSE,CoreRules.PROJECT_MERGE,CoreRules.PROJECT_MULTI_JOIN_MERGE, CoreRules.PROJECT_REMOVE,CoreRules.PROJECT_TO_CALC,CoreRules.PROJECT_SUB_QUERY_TO_CORRELATE,CoreRules.PROJECT_REDUCE_EXPRESSIONS,PruneEmptyRules.PROJECT_INSTANCE),
            "rule_cal", List.of(CoreRules.CALC_MERGE,CoreRules.CALC_REMOVE),
            "rule_orderby", List.of(CoreRules.SORT_JOIN_TRANSPOSE,CoreRules.SORT_PROJECT_TRANSPOSE,CoreRules.SORT_UNION_TRANSPOSE,CoreRules.SORT_REMOVE_CONSTANT_KEYS,CoreRules.SORT_REMOVE,PruneEmptyRules.SORT_INSTANCE,PruneEmptyRules.SORT_FETCH_ZERO_INSTANCE),
            "rule_union", List.of(CoreRules.UNION_MERGE,CoreRules.UNION_REMOVE,CoreRules.UNION_TO_DISTINCT,CoreRules.UNION_PULL_UP_CONSTANTS,PruneEmptyRules.UNION_INSTANCE,PruneEmptyRules.INTERSECT_INSTANCE,PruneEmptyRules.MINUS_INSTANCE)
    );

    public QueryRewriter(SchemaPlus schema, List<String> rules) {
        this.relToSqlConverter = new RelToSqlConverter(sqlDialect);

        HepProgramBuilder hepProgramBuilder = new HepProgramBuilder();
        for (String ruleClass : rules) {
            if (this.ruleMap.containsKey(ruleClass)) {
                for (RelOptRule ruleInstance : this.ruleMap.get(ruleClass)) {
                    hepProgramBuilder.addRuleInstance(ruleInstance);
                }
            } else {
                System.out.println("Rule class " + ruleClass + " is not available.");
            }
        }
        hepProgramBuilder.addMatchOrder(HepMatchOrder.TOP_DOWN);
        this.hepPlanner = new HepPlanner(hepProgramBuilder.build());

        SqlParser.Config parserConfig = SqlParser.config().withLex(Lex.MYSQL).withUnquotedCasing(UNCHANGED).withCaseSensitive(false).withQuoting(DOUBLE_QUOTE).withConformance(SqlConformanceEnum.MYSQL_5);
        FrameworkConfig config = Frameworks.newConfigBuilder().parserConfig(parserConfig).defaultSchema(schema).build();
        this.planner = Frameworks.getPlanner(config);
    }

    public RelNode sqlToRelNode(String sql) throws SqlParseException, ValidationException, RelConversionException {
        sql = sql.replaceAll(";", "").replaceAll("\n", " ");

        this.planner.close();
        this.planner.reset();

        SqlNode sqlNode = this.planner.parse(sql);
        sqlNode = this.planner.validate(sqlNode);

        return this.planner.rel(sqlNode).project();
    }

    public RelNode applyRules(RelNode logicalPlan) {
        RelNode finalNode = logicalPlan;
        for (int i = 0;i < 5;i++){
            this.hepPlanner.setRoot(finalNode);
            finalNode = this.hepPlanner.findBestExp();
        }
        return finalNode;
    }

    public RelNode applyRules(String sql) throws SqlParseException, ValidationException, RelConversionException {
        RelNode logicalPlan = sqlToRelNode(sql);
        return applyRules(logicalPlan);
    }

    public String rewrite(String sql) throws SqlParseException, ValidationException, RelConversionException {
        RelNode relNode = applyRules(sql);
        return this.relToSqlConverter.visitRoot(relNode)
                .asStatement()
                .toSqlString(this.sqlDialect)
                .getSql();
    }

    public List<RelOptRule> getRules(String ruleClass) {
        assert this.ruleMap.containsKey(ruleClass);
        return this.ruleMap.get(ruleClass);
    }

    public List<RelOptRule> getAllRules() {
        List<RelOptRule> rules = new ArrayList<>();
        for (String key : this.ruleMap.keySet()) {
            rules.addAll(this.ruleMap.get(key));
        }
        return rules;
    }
}
