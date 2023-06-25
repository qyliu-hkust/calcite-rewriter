import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.parser.SqlParseException;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import schema.BuildinSchema;
import testquery.QueryUtils;
import testquery.TpchQuery;

import java.io.FileReader;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.List;

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

    private static void loadJSON(String path) {
        JSONParser jsonParser = new JSONParser();
        try (FileReader reader = new FileReader(path)) {
            JSONArray parse = (JSONArray) jsonParser.parse(reader);
            for (int i=0; i<parse.size(); ++i) {
                JSONObject rewrite = (JSONObject) parse.get(i);
                JSONArray rewrites = (JSONArray) rewrite.get("rewrites");
                for (int j=0; j<rewrites.size(); ++j) {
                    JSONObject rewriteQuery = (JSONObject) rewrites.get(j);
                    System.out.println((String) rewriteQuery.get("rewrite_query"));
                    System.out.println("================================");
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws SqlParseException {
//        loadJSON("./tpch_rewrite.json");
        List<List<String>> ruleClasses = Utils.enumRuleClassesByRandomChosen(1000);

        SchemaPlus tpchSchema = BuildinSchema.getTpchSchema();
        List<String> tpchQueries = QueryUtils.getTpchQueries();
        rewriteToJSON("./tpch_rewrite.json", tpchQueries, tpchSchema, ruleClasses);

        SchemaPlus statsSchema = BuildinSchema.getStatsSchema();
        List<String> statsQueries = QueryUtils.getStatsQueries();
        rewriteToJSON("./stats_rewrite.json", statsQueries, statsSchema, ruleClasses);

        SchemaPlus imdbSchema = BuildinSchema.getImdbSchema();
        List<String> imdbQueries = QueryUtils.getImdbQueries();
        rewriteToJSON("./imdb_rewrite.json", imdbQueries, imdbSchema, ruleClasses);
    }
}
