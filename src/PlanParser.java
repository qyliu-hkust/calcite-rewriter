import models.Operator;
import models.Plan;
import org.apache.calcite.util.Pair;
import org.checkerframework.checker.units.qual.A;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.*;
import java.util.stream.Collectors;

public class PlanParser {
    private final Map<String, Integer> tableMap;

    public PlanParser(Map<String, Integer> tableMap) {
        this.tableMap = tableMap;
    }

    public Pair<Plan, List<Operator>> parse(String explainJSONString, int planId, int queryId, int opFirstId)
            throws ParseException {
        JSONParser jsonParser = new JSONParser();
        JSONArray parse = (JSONArray) jsonParser.parse(explainJSONString);
        JSONObject planJSONParse = (JSONObject) parse.get(0);
        planJSONParse = (JSONObject) planJSONParse.get("Plan");

        TreeNode planRoot = readPlanNode(planJSONParse);
        List<TreeNode> treeNodes = treeToNodeList(planRoot, opFirstId);

        Plan plan = new Plan(planId, queryId, planRoot.estCost, (int) planRoot.estRows,
                planRoot.actualTime, (int) planRoot.actualRows);


        List<Operator> operators = new ArrayList<>(treeNodes.size());
        int currId = opFirstId;
        for (TreeNode node : treeNodes) {
            int tableId = -1;
            if (node.tableName != null) {
                tableId = this.tableMap.get(node.tableName);
            }
            Operator operator = new Operator(currId, tableId, planId, node.leftChildId, node.rightChildId,
                    node.nodeType, node.estCost, (int) node.estRows, node.actualTime, (int) node.actualRows,
                    (int) node.actualLoops, (int) node.hitBlocks, (int) node.readBlocks);
            operators.add(operator);
            currId ++;
        }

        return Pair.of(plan, operators);
    }

    public Pair<List<Plan>, List<Operator>> parseFromFiles(String path, String prefix, int maxQueryId) {
        String pathFormat = path + prefix + "_%d.json";
        int opCurrId = 0;
        int planCurrId = 0;

        List<Plan> plans = new ArrayList<>();
        List<Operator> operators = new ArrayList<>();

        for (int queryId=1; queryId<=maxQueryId; ++queryId) {
            try (FileReader reader = new FileReader(pathFormat.formatted(queryId))) {
                JSONParser jsonParser = new JSONParser();
                JSONArray parse = (JSONArray) jsonParser.parse(reader);

                for (Object plan : parse) {
                    JSONArray jsonArray = (JSONArray) ((JSONObject) plan).get("explain_json_string");
                    if (jsonArray.size() == 0) {
                        continue;
                    }
                    String explainJSONString = (String) jsonArray.get(0);
                    Pair<Plan, List<Operator>> plansAndOperators = parse(explainJSONString, planCurrId, queryId, opCurrId);
                    plans.add(plansAndOperators.left);
                    assert plansAndOperators.right != null;
                    operators.addAll(plansAndOperators.right);

                    planCurrId ++;
                    opCurrId += plansAndOperators.right.size();
                }
            } catch (IOException | ParseException e) {
                e.printStackTrace();
            }
        }

        return Pair.of(plans, operators);
    }

    public void parseFromFileToCSV(String path, String prefix, int maxQueryId, String outputPath) {
        Pair<List<Plan>, List<Operator>> plansAndOperators = parseFromFiles(path, prefix, maxQueryId);
        assert plansAndOperators.left != null;
        assert plansAndOperators.right != null;

        System.out.println("write plans to " + outputPath + "plans.csv");
        try (PrintWriter writer = new PrintWriter(new FileWriter(outputPath + "plans.csv"))) {
            writer.println(Plan.getMetaString());
            for (Plan plan : plansAndOperators.left) {
                writer.println(plan.toString());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        System.out.println("write operators to " + outputPath + "operators.csv");
        try (PrintWriter writer = new PrintWriter(new FileWriter(outputPath + "operators.csv"))) {
            writer.println(Operator.getMetaString());
            for (Operator operator : plansAndOperators.right) {
                writer.println(operator.toString());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static TreeNode readPlanNode(JSONObject parse) {
        return readPlanNode(parse, 0);
    }

    private static TreeNode readPlanNode(JSONObject parse, int i) {
        String nodeType = (String) parse.get("Node Type");
        double estCost = (double) parse.get("Total Cost");
        long estRows = (long) parse.get("Plan Rows");
        double actualTime = (double) parse.get("Actual Total Time");
        long actualRows = (long) parse.get("Actual Rows");
        long actualLoops = (long) parse.get("Actual Loops");
        long hitBlocks = (long) parse.get("Shared Hit Blocks");
        long readBlocks = (long) parse.get("Shared Read Blocks");

        TreeNode node = new TreeNode(nodeType, estCost, estRows,
                actualTime, actualRows, actualLoops, hitBlocks, readBlocks);

        // can be null
        node.tableName = (String) parse.get("Relation Name");

        JSONArray plans = (JSONArray) parse.get("Plans");
        if (plans != null) {
            if (plans.size() == 1) {
                node.left = readPlanNode((JSONObject) plans.get(0), i+1);
            } else if (plans.size() == 2) {
                node.left = readPlanNode((JSONObject) plans.get(0), i+1);
                node.right = readPlanNode((JSONObject) plans.get(1), i+1);
            } else {
                throw new RuntimeException("Bad tree size.");
            }
        }

        return node;
    }

    private static List<TreeNode> treeToNodeList(TreeNode root, int firstId) {
        List<TreeNode> nodeList = levelTraverse(root);
        for (int i=0; i<nodeList.size(); ++i) {
            nodeList.get(i).id = firstId + i;
        }

        for (TreeNode node : nodeList) {
            if (node.left != null) {
                node.leftChildId = node.left.id;
            }
            if (node.right != null) {
                node.rightChildId = node.right.id;
            }
        }

        return nodeList;
    }

    static class TreeNode {
        TreeNode left;
        TreeNode right;
        String nodeType;
        double estCost;
        long estRows;
        double actualTime;
        long actualRows;
        long actualLoops;
        long hitBlocks;
        long readBlocks;
        String tableName;

        int id;
        int leftChildId;
        int rightChildId;

        TreeNode(String nodeType, double estCost, long estRows,
                 double actualTime, long actualRows, long actualLoops,
                 long hitBlocks, long readBlocks) {
            this.nodeType = nodeType;
            this.estCost = estCost;
            this.estRows = estRows;
            this.actualTime = actualTime;
            this.actualRows = actualRows;
            this.actualLoops = actualLoops;
            this.hitBlocks = hitBlocks;
            this.readBlocks = readBlocks;

            this.tableName = null;
            this.left = null;
            this.right = null;

            this.id = -1;
            this.leftChildId = -1;
            this.rightChildId = -1;
        }

        @Override
        public String toString() {
            return "%s (id=%d, lid=%d, rid=%d)"
                    .formatted(this.nodeType, this.id, this.leftChildId, this.rightChildId);
        }
    }

    private static List<TreeNode> levelTraverse(TreeNode root) {
        Queue<TreeNode> queue = new LinkedList<>();
        List<TreeNode> nodeList = new ArrayList<>();

        queue.add(root);
        while (! queue.isEmpty()) {
            TreeNode visit = queue.poll();
            nodeList.add(visit);

            if (visit.left != null) {
                queue.add(visit.left);
            }

            if (visit.right != null) {
                queue.add(visit.right);
            }
        }

        return nodeList;
    }

    public static void main(String[] args) {
        Map<String, Integer> tableMap = Map.of(
                "badges", 0,
                "comments", 1,
                "posthistory", 2,
                "postlinks", 3,
                "posts", 4,
                "tags", 5,
                "users", 6,
                "votes", 7
        );

        PlanParser planParser = new PlanParser(tableMap);
        planParser.parseFromFileToCSV("./results/", "stats", 35, "./");
    }
}
