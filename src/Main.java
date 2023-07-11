import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.tools.RelConversionException;
import org.apache.calcite.tools.ValidationException;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import schema.BuildinSchema;
import testquery.QueryPreprossor;
import testquery.QueryUtils;

import java.io.FileReader;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Main {

    private static void rewriteToJSON(String path, List<String> sqlList, SchemaPlus schema, List<List<String>> ruleClasses) {
        JSONArray jsonArray = new JSONArray();

        for (String sql : sqlList) {
            System.out.println("=========================");
            System.out.println(sql);
            JSONObject json = Utils.rewriteToJSON(schema, sql, ruleClasses);
            jsonArray.add(json);
            System.out.println("Generate " + json.get("num_rewrites") + " rewrite queries");
            System.out.println("=========================");
        }

        try (PrintWriter out = new PrintWriter(new FileWriter(path))) {
            out.write(jsonArray.toString());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static Map<String, List<String>> loadJSON(String path) {
        JSONParser jsonParser = new JSONParser();
        Map<String, List<String>> queryMap = new HashMap<>();

        try (FileReader reader = new FileReader(path)) {
            JSONArray parse = (JSONArray) jsonParser.parse(reader);
            for (int i=0; i<parse.size(); ++i) {
                JSONObject rewrite = (JSONObject) parse.get(i);
                String rawSql = (String) rewrite.get("raw_query");

                JSONArray rewrites = (JSONArray) rewrite.get("rewrites");
                List<String> rewriteList = new ArrayList<>();
                for (int j=0; j<rewrites.size(); ++j) {
                    JSONObject rewriteQuery = (JSONObject) rewrites.get(j);
                    String sql = new QueryPreprossor((String) rewriteQuery.get("rewrite_query"))
//                            .removeDoubleQuotation()
                            .removeEscapeCharacter()
                            .removeSemicolon()
                            .toString();
                    rewriteList.add(sql);
                }

                queryMap.put(rawSql, rewriteList);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        return queryMap;
    }

    public static void main(String[] args) throws ValidationException, SqlParseException, RelConversionException {
        Map<String, List<String>> queryMap = loadJSON("./stats_rewrite.json");
        QueryRunner queryRunner = new QueryRunner("stats", "public", "postgres", "Liuqiyu1995");
        try {
            queryRunner.connect();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        Integer id = 0;
        JSONObject idMap = new JSONObject();
        for (String rawQuery : queryMap.keySet()) {
            idMap.put(id, rawQuery);
            id ++;
//            JSONArray runBatch = queryRunner.runBatch(queryMap.get(rawQuery), 1);
//
//            try (PrintWriter out = new PrintWriter(new FileWriter("./results/stats_" + id + ".json"))) {
//                out.write(runBatch.toString());
//            } catch (Exception e) {
//                e.printStackTrace();
//            }
        }

        try (PrintWriter out = new PrintWriter(new FileWriter("./results/stats_queries.json"))) {
            out.write(idMap.toString());
        } catch (Exception e) {
            e.printStackTrace();
        }

//        List<List<String>> ruleClasses = Utils.enumRuleClassesByRandomChosen(1000 );
//
//        SchemaPlus tpchSchema = BuildinSchema.getTpchSchema();
//        List<String> tpchQueries = QueryUtils.getTpchQueries();
//        rewriteToJSON("./tpch_rewrite.json", tpchQueries, tpchSchema, ruleClasses);
//


//        List<String> statsQueries = QueryUtils.getStatsQueries();
//        rewriteToJSON("./stats_rewrite.json", statsQueries, statsSchema, ruleClasses);
//
//        SchemaPlus imdbSchema = BuildinSchema.getImdbSchema();
//        List<String> imdbQueries = QueryUtils.getImdbQueries();
//        rewriteToJSON("./imdb_rewrite.json", imdbQueries, imdbSchema, ruleClasses);

//        SchemaPlus statsSchema = BuildinSchema.getStatsSchema();
//        QueryRewriter rewriter = new QueryRewriter(statsSchema, List.of("rule_filter", "rule_union", "rule_orderby", "rule_agg", "rule_cal", "rule_join"));
//        String sql = "SELECT COUNT(*) FROM comments as c, posts as p, users as u WHERE u.Id = p.OwnerUserId AND c.UserId = u.Id AND c.CreationDate>='2010-07-27 17:46:38' AND p.AnswerCount>=0 AND p.AnswerCount<=4 AND p.CommentCount>=0 AND p.CommentCount<=11 AND p.CreationDate>='2010-07-26 09:46:48' AND p.CreationDate<='2014-09-13 10:09:50' AND u.Reputation>=1 AND u.CreationDate>='2010-08-03 19:42:40' AND u.CreationDate<='2014-09-12 02:20:03';";
//        System.out.println(rewriter.rewrite(sql));
    }
}
