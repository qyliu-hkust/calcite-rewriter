package testquery;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class QueryUtils {
    public static List<String> loadQueries(String path, String split, int index) {
        List<String> queries = new ArrayList<>();

        try (BufferedReader reader = new BufferedReader(new FileReader(path))) {
            String line;
            while ((line = reader.readLine()) != null) {
                String sql = line.split(split)[index];
                sql = sql.replaceAll("::timestamp", "");
                queries.add(sql);
            }

        } catch (IOException e) {
            e.printStackTrace();
        }

        return queries;
    }

    public static List<String> getStatsQueries() {
        return loadQueries("src/testquery/stats.txt", "#####", 1);
    }

    public static List<String> getTpchQueries() {
        return loadQueries("src/testquery/tpch.txt", "#####", 1);
    }

    public static List<String> getImdbQueries() {
        return loadQueries("src/testquery/job.txt", "#####", 1);
    }
}
