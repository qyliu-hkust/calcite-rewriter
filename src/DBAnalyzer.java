import models.Column;
import models.Table;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.*;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DBAnalyzer {
    private final String url;
    private final String user;
    private final String password;
    private final String schema;
    private Connection conn = null;

    public DBAnalyzer(String database, String schema, String user, String password) {
        this.url = "jdbc:postgresql://localhost:5433/" + database + "?currentSchema=" + schema;
        this.user = user;
        this.password = password;
        this.schema = schema;
    }

    public void connect() throws SQLException {
        this.conn = DriverManager.getConnection(url, user, password);
    }

    public List<Table> getTables() throws SQLException {
        Map<String, TableCount> tableCountMap = getTableCount();

        String sql = "SELECT tablename FROM pg_catalog.pg_tables where schemaname = '%s'".formatted(schema);
        Statement s = this.conn.createStatement();
        ResultSet rs = s.executeQuery(sql);

        List<Table> tables = new ArrayList<>();
        int currId = 0;

        while (rs.next()) {
            String tableName = rs.getString("tablename");
            Table table = new Table(currId, tableName);
            table.setRowNum(tableCountMap.get(tableName).rowCount);
            table.setColNum(tableCountMap.get(tableName).columnCount);

            tables.add(table);
            currId ++;
        }

        rs.close();
        s.close();

        return tables;
    }

    public List<Column> getColumns(List<Table> tables) throws SQLException {
        Map<String, Integer> tableMap = new HashMap<>();
        for (Table table : tables) {
            tableMap.put(table.getTableName(), table.getTableId());
        }

        Map<String, List<String>> indexes = getIndexes();
        Map<String, Map<String, Stats>> stats = getStats();

        String sql = "select table_name, column_name, data_type\n" +
                "from INFORMATION_SCHEMA.COLUMNS where table_name in \n" +
                "\t(SELECT tablename FROM pg_catalog.pg_tables where schemaname = '%s')\n" +
                "order by table_name";
        sql = sql.formatted(schema);

        Statement s = this.conn.createStatement();
        ResultSet rs = s.executeQuery(sql);

        List<Column> columns = new ArrayList<>();
        int currId = 0;

        while (rs.next()) {
            String tableName = rs.getString("table_name");
            String columnName = rs.getString("column_name");
            String dataType = rs.getString("data_type");

            int tableId = tableMap.get(tableName);
            int hasIndex = 0;

            for (String indexedColumn : indexes.get(tableName)) {
                if (columnName.equals(indexedColumn)) {
                    hasIndex = 1;
                    break;
                }
            }

            Column column = new Column(currId, tableId, columnName, parseType(dataType), hasIndex);
            Stats colStats = stats.get(tableName).get(columnName);
            if (colStats != null) {
                column.setWidth(colStats.width);
                column.setDistinct(colStats.distinct);
                column.setNullFrac(colStats.nullFrac);
                column.setCorrelation(colStats.correlation);
            }
            columns.add(column);
            currId ++;
        }

        return columns;
    }

    public void toCSV(String path) throws SQLException {
        List<Table> tables = getTables();
        List<Column> columns = getColumns(tables);

        // write table to csv file
        System.out.println("Write table to " + path + "table.csv");
        try (PrintWriter writer = new PrintWriter(new FileWriter(path + "table.csv"))) {
            writer.println(Table.getMetaString());
            for (Table table : tables) {
                writer.println(table.toString());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        // write column to csv file
        System.out.println("Write column to " + path + "column.csv");
        try (PrintWriter writer = new PrintWriter(new FileWriter(path + "column.csv"))) {
            writer.println(Column.getMetaString());
            for (Column column : columns) {
                writer.println(column.toString());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private Map<String, List<String>> getIndexes() throws SQLException {
        String sql = "select tablename, indexdef\n" +
                "from pg_indexes where schemaname = '%s'\n" +
                "order by tablename";
        sql = sql.formatted(this.schema);

        Statement s = this.conn.createStatement();
        ResultSet rs = s.executeQuery(sql);

        Map<String, List<String>> indexes = new TreeMap<>();
        Pattern compile = Pattern.compile("\\((.*?)\\)");

        while (rs.next()) {
            String tableName = rs.getString("tablename");
            String indexDef = rs.getString("indexdef");

            Matcher matcher = compile.matcher(indexDef);
            String indexedColumn = null;

            if (matcher.find()) {
                indexedColumn = matcher.group(1);
            }

            if (indexes.containsKey(tableName)) {
                indexes.get(tableName).add(indexedColumn);
            } else {
                List<String> indexDefList = new ArrayList<>();
                indexDefList.add(indexedColumn);
                indexes.put(tableName, indexDefList);
            }
        }

        rs.close();
        s.close();

        return indexes;
    }

    static class Stats {
        int width;
        int distinct;
        double nullFrac;
        double correlation;

        Stats(int width, int distinct, double nullFrac, double correlation) {
            this.width = width;
            this.distinct = distinct;
            this.nullFrac = nullFrac;
            this.correlation = correlation;
        }

        @Override
        public String toString() {
            return "Stats{" +
                    "width=" + width +
                    ", distinct=" + distinct +
                    ", nullFrac=" + nullFrac +
                    ", correlation=" + correlation +
                    '}';
        }
    }

    public Map<String, Map<String, Stats>> getStats() throws SQLException {
        String sql = "select tablename, attname as colname, null_frac, avg_width as width, n_distinct, correlation " +
                "from pg_stats where schemaname = '%s'";
        sql = sql.formatted(this.schema);

        Statement s = this.conn.createStatement();
        ResultSet rs = s.executeQuery(sql);

        Map<String, Map<String, Stats>> statsMap = new TreeMap<>();
        while (rs.next()) {
            String tableName = rs.getString("tablename");
            String columnName = rs.getString("colname");
            int width = rs.getInt("width");
            double rawDistinct = rs.getDouble("n_distinct");
            int distinct = rawDistinct > 0 ? (int) rawDistinct : -1;
            double nullFrac = rs.getDouble("null_frac");
            double correlation = rs.getDouble("correlation");

            if (statsMap.containsKey(tableName)) {
                statsMap.get(tableName).put(columnName, new Stats(width, distinct, nullFrac, correlation));
            } else {
                statsMap.put(tableName, new TreeMap<>(
                        Map.of(columnName, new Stats(width, distinct, nullFrac, correlation)))
                );
            }
        }

        rs.close();
        s.close();

        return statsMap;
    }

    static class TableCount {
        int rowCount;
        int columnCount;

        TableCount(int rowCount, int columnCount) {
            this.rowCount = rowCount;
            this.columnCount = columnCount;
        }
    }

    private Map<String, TableCount> getTableCount() throws SQLException {
        String sql = "select table_name, count(*) from information_schema.columns\n" +
                "where table_name in (\n" +
                "SELECT tablename FROM pg_catalog.pg_tables where schemaname = '%s')\n" +
                "group by table_name";
        sql = sql.formatted(this.schema);

        Statement s = this.conn.createStatement();
        ResultSet rs = s.executeQuery(sql);

        Map<String, TableCount> tableCountMap = new TreeMap<>();
        while (rs.next()) {
            String tableName = rs.getString("table_name");
            int columnCount = rs.getInt("count");
            tableCountMap.put(tableName, new TableCount(0, columnCount));
        }

        for (String tableName : tableCountMap.keySet()) {
            String countQuery = "select count(*) from %s".formatted(tableName);
            rs = s.executeQuery(countQuery);

            if (rs.next()) {
                tableCountMap.get(tableName).rowCount = rs.getInt("count");
            }
        }

        return tableCountMap;
    }

    // 0: float, 1: integer, 2: date, 3: text 4: others
    private int parseType(String dataType) {
        dataType = dataType.toLowerCase();
        if (dataType.equals("real") || dataType.equals("double precision")) {
            return 0;
        }
        if (dataType.equals("smallint") || dataType.equals("integer") || dataType.equals("bigint")) {
            return 1;
        }
        if (dataType.contains("date")) {
            return 2;
        }
        if (dataType.contains("varchar")) {
            return 3;
        }
        return 4;
    }

    public static void main(String[] args) throws SQLException {
        DBAnalyzer dbAnalyzer = new DBAnalyzer("stats", "public", "postgres", "Liuqiyu1995");
        dbAnalyzer.connect();
        dbAnalyzer.toCSV("./");
//        List<Table> tables = dbAnalyzer.getTables();
//        List<Column> columns = dbAnalyzer.getColumns(tables);
//        tables.forEach(System.out::println);
//        columns.forEach(System.out::println);
    }
}
