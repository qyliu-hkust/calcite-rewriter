import org.apache.calcite.schema.SchemaPlus;
import org.json.simple.JSONObject;
import schema.BuildinSchema;
import testquery.TpchQuery;

import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.List;

public class Main {
    public static void main(String[] args) {

        String writePath = "./q1_rewrite.json";
        SchemaPlus tpchSchema = BuildinSchema.getTpchSchema();
        List<List<String>> ruleClasses = Utils.enumRandomRuleClasses(10);

        System.out.println("=========================");
        String sql = TpchQuery.Q1;
        System.out.println(sql);
        System.out.println("=========================");
        JSONObject json = Utils.rewriteToJSON(tpchSchema, sql, ruleClasses);
        System.out.println("Write rewrite queries to " + writePath);

        try (PrintWriter out = new PrintWriter(new FileWriter(writePath))) {
            out.write(json.toString());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}