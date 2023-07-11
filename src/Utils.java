import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlExplainFormat;
import org.apache.calcite.sql.SqlExplainLevel;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import java.util.*;

public class Utils {
    public static List<String> getRuleClassesInRandomOrder() {
        List<String> allRuleClasses = new ArrayList<>(
                List.of("rule_agg", "rule_filter", "rule_join", "rule_project", "rule_cal", "rule_orderby", "rule_union"));
        Collections.shuffle(allRuleClasses);
        return allRuleClasses;
    }

    public static List<List<String>> enumRandomRuleClasses(int n) {
        List<List<String>> res = new ArrayList<>(n);
        for (int i=0; i<n; ++i) {
            res.add(getRuleClassesInRandomOrder());
        }
        return res;
    }

    public static List<List<String>> enumAllRuleClasses() {
        List<String> allRuleClasses = new ArrayList<>(
                List.of("rule_agg", "rule_filter", "rule_join", "rule_project", "rule_cal", "rule_orderby", "rule_union"));
        return permute(allRuleClasses);
    }

    public static List<String> getRuleClassesByRandomChosen() {
        List<String> ruleClasses = getRuleClassesInRandomOrder();
        Random random = new Random();
        int index = random.nextInt(0, ruleClasses.size());
        return ruleClasses.subList(0, index);
    }

    public static List<List<String>> enumRuleClassesByRandomChosen(int n) {
        List<List<String>> res = new ArrayList<>(n);
        for (int i=0; i<n; ++i) {
            res.add(getRuleClassesByRandomChosen());
        }
        return res;
    }

    private static  <E> List<List<E>> permute(List<E> original) {
        if (original.isEmpty()) {
            List<List<E>> result = new ArrayList<>();
            result.add(new ArrayList<>());
            return result;
        }
        E firstElement = original.remove(0);
        List<List<E>> returnValue = new ArrayList<>();
        List<List<E>> permutations = permute(original);
        for (List<E> smallerPermutated : permutations) {
            for (int index = 0; index <= smallerPermutated.size(); index++) {
                List<E> temp = new ArrayList<>(smallerPermutated);
                temp.add(index, firstElement);
                returnValue.add(temp);
            }
        }
        return returnValue;
    }

    public static String planToText(RelNode relNode, String header) {
        return RelOptUtil.dumpPlan(
                header,
                relNode,
                SqlExplainFormat.TEXT,
                SqlExplainLevel.NON_COST_ATTRIBUTES);
    }

    public static JSONObject rewriteToJSON(SchemaPlus schema, String sql, List<List<String>> ruleClasses) {
        JSONObject json = new JSONObject();
        json.put("raw_query", sql);

        JSONArray rewrites = new JSONArray();
        Set<String> rewriteSet = new HashSet<>();

        try {
            for (List<String> ruleClass : ruleClasses) {
                long start = System.nanoTime();
                QueryRewriter rewriter = new QueryRewriter(schema, ruleClass);
                String rewrite = rewriter.rewrite(sql);
                long end = System.nanoTime();

                if (!rewriteSet.contains(rewrite)) {
                    rewriteSet.add(rewrite);

                    JSONObject rewriteJSON = new JSONObject();
                    rewriteJSON.put("rule_order", ruleClass.toString());
                    rewriteJSON.put("rewrite_time", (end - start) / 1e6);
                    rewriteJSON.put("rewrite_query", rewrite);

                    rewrites.add(rewriteJSON);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        json.put("rewrites", rewrites);
        json.put("num_rewrites", rewrites.size());
        return json;
    }


}
