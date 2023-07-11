import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import java.io.FileWriter;
import java.io.PrintWriter;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class QueryRunner {
    private final String url;
    private final String user;
    private final String password;
    private Connection conn = null;

    public QueryRunner(String database, String schema, String user, String password) {
        this.url = "jdbc:postgresql://localhost:5433/" + database + "?currentSchema=" + schema;
        this.user = user;
        this.password = password;
    }

    public void connect() throws SQLException {
        this.conn = DriverManager.getConnection(url, user, password);
    }

    public void close() throws SQLException {
        this.conn.close();
    }

    public String runAndAnalyze(String sql, String pgAugments) throws SQLException {
        String explain = "explain (ANALYZE, VERBOSE, COSTS, BUFFERS, TIMING, SUMMARY, FORMAT JSON) " + sql;
        Statement ps = conn.createStatement();

        if (pgAugments != null) {
            ps.executeUpdate(pgAugments);
        }

        ResultSet rs = ps.executeQuery(explain);
        String jsonString = null;
        if (rs.next()) {
             jsonString = rs.getString(1);
             rs.close();
             ps.close();
        } else {
            rs.close();
            ps.close();
            throw new RuntimeException("Failed to run query " + explain);
        }

        return jsonString;
    }

    public List<String> runAndAnalyze(String sql, String pgAugments, int repeat) throws SQLException {
        List<String> result = new ArrayList<>(repeat);
        for (int i=0; i<repeat; ++i) {
            result.add(runAndAnalyze(sql, pgAugments));
        }
        return result;
    }

    public JSONArray runBatch(List<String> queries, int repeat) {
        JSONArray batchResult = new JSONArray();

        List<String> configs = generatePgPlannerConfig();
        int total = queries.size() * configs.size();
        int count = 0;
        for (String sql : queries) {
            for (String config : configs) {
                JSONObject explainJSON = new JSONObject();
                explainJSON.put("sql", sql);
                explainJSON.put("config", config);
                explainJSON.put("repeat", repeat);

                List<String> explainResults = null;
                try {
                    explainResults = runAndAnalyze(sql, config, repeat);
                } catch (SQLException e) {
                    e.printStackTrace();
                    System.out.println(sql);
                }

                JSONArray explainJSONArray = new JSONArray();

                if (explainResults != null) {
                    for (String explain : explainResults) {
                        explainJSONArray.add(explain);
                    }
                }

                explainJSON.put("explain_json_string", explainJSONArray);
                batchResult.add(explainJSON);

                count ++;
                System.out.print("Finished " + count + " / " + total);
                System.out.println("\r");
            }
        }

        return batchResult;
    }

    private static List<String> generatePgPlannerConfig() {
        String defaultJoinPrefix = "set enable_hashjoin  to true; " +
                                   "set enable_mergejoin to true; " +
                                   "set enable_nestloop  to true; ";
        String hashJoinPrefix = "set enable_hashjoin  to true; " +
                                "set enable_mergejoin to false; " +
                                "set enable_nestloop  to false; ";
        String mergeJoinPrefix = "set enable_hashjoin  to false; " +
                                 "set enable_mergejoin to true; " +
                                 "set enable_nestloop  to false; ";
        String nestedLoopJoinPrefix = "set enable_hashjoin  to false; " +
                                      "set enable_mergejoin to false; " +
                                      "set enable_nestloop  to true; ";

        List<String> prefixes = List.of(defaultJoinPrefix, hashJoinPrefix, mergeJoinPrefix, nestedLoopJoinPrefix);

        List<String> configs = new ArrayList<>();

        List<List<Boolean>> permutes = permuteBooleanList(5);
        for (List<Boolean> permute : permutes) {
            PGConfigBuilder configBuilder = new PGConfigBuilder();
            configBuilder.set("enable_hashagg", permute.get(0));
            configBuilder.set("enable_material", permute.get(1));
            configBuilder.set("enable_memoize", permute.get(2));
            configBuilder.set("enable_sort", permute.get(3));
            configBuilder.set("geqo", permute.get(4));

            for (String prefix : prefixes) {
                configs.add(prefix + configBuilder.build());
            }
        }

        return configs;
    }

    private static class PGConfigBuilder {
        StringBuilder sb;


        PGConfigBuilder() {
            sb = new StringBuilder();
        }

        PGConfigBuilder set(String key, Object value) {
            sb.append("set ").append(key).append(" to ").append(value).append("; ");
            return this;
        }

        String build() {
            return sb.toString();
        }
    }

    private static List<List<Boolean>> permuteBooleanList(int n) {
        List<List<Boolean>> permute = new ArrayList<>();
        // from 0 to 2^n-1 i.e., 00000 ... 11111
        for (int i=0; i<Math.pow(2, n); ++i) {
            String binaryString = Integer.toBinaryString(i);
            if (binaryString.length() < n) {
                binaryString = "0".repeat(n - binaryString.length()) + binaryString;
            }
            List<Boolean> booleanList = new ArrayList<>(n);
            for (char bool : binaryString.toCharArray()) {
                booleanList.add(bool == '0');
            }
            permute.add(booleanList);
        }
        return permute;
    }

    public static void main(String[] args) throws SQLException {
        QueryRunner queryRunner = new QueryRunner("stats", "public", "postgres", "Liuqiyu1995");
        queryRunner.connect();
        String sql = "SELECT COALESCE(SUM(t3.$f4 * t5.EXPR$0), 0)  FROM (SELECT t2.OwnerUserId, t0.EXPR$0 * t2.EXPR$0 AS $f4  FROM (SELECT UserId, COUNT(*) AS EXPR$0  FROM comments  WHERE CreationDate >= '2010-07-27 17:46:38'  GROUP BY UserId) AS t0  INNER JOIN (SELECT OwnerUserId, COUNT(*) AS EXPR$0  FROM posts  WHERE AnswerCount >= 0 AND AnswerCount <= 4 AND (CommentCount >= 0 AND CommentCount <= 11) AND CreationDate >= '2010-07-26 09:46:48' AND CreationDate <= '2014-09-13 10:09:50'  GROUP BY OwnerUserId) AS t2 ON t0.UserId = t2.OwnerUserId) AS t3  INNER JOIN (SELECT Id, COUNT(*) AS EXPR$0  FROM users  WHERE Reputation >= 1 AND CreationDate >= '2010-08-03 19:42:40' AND CreationDate <= '2014-09-12 02:20:03'  GROUP BY Id) AS t5 ON t3.OwnerUserId = t5.Id;";
        JSONArray jsonArray = queryRunner.runBatch(List.of(sql), 1);

        try (PrintWriter out = new PrintWriter(new FileWriter("res.json"))) {
            out.write(jsonArray.toString());
        } catch (Exception e) {
            e.printStackTrace();
        }

//        List<String> explainResult = queryRunner.runAndAnalyze(sql, "set enable_hashjoin to true;set enable_mergejoin to false;", 5);
//        System.out.println(explainResult);
//        queryRunner.close();
    }
}
