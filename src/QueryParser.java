import models.Query;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.util.SqlBasicVisitor;
import org.apache.calcite.sql.util.SqlVisitor;
import org.apache.calcite.tools.RelConversionException;
import org.apache.calcite.tools.ValidationException;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import schema.BuildinSchema;

import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.*;

public class QueryParser {
    private final QueryRewriter rewriter;

    public QueryParser(SchemaPlus schema) {
        this.rewriter = new QueryRewriter(schema, List.of());
    }

    public Query parse(String sql, int id) throws ValidationException, SqlParseException, RelConversionException {
        RelNode root = this.rewriter.sqlToRelNode(sql);
        int joinCount = getJoinCount(root);
        int isAggregate = isAggregate(root);
        int tableCount = getTableCount(root);
        QueryStats queryStats = getQueryStats(sql);

        return new Query(id, sql, tableCount, queryStats.selectListSize, isAggregate, joinCount,
                queryStats.isFilterInWhere, queryStats.isGroupBy, queryStats.isHaving,
                queryStats.isOrderBy, queryStats.isDistinct);
    }

    public List<Query> parseFromFile(String path) throws ValidationException, SqlParseException, RelConversionException {
        List<Query> queries = new ArrayList<>();
        try (FileReader reader = new FileReader(path)) {
            JSONParser jsonParser = new JSONParser();
            JSONObject parse = (JSONObject) jsonParser.parse(reader);

            for (Object key : parse.keySet()) {
                int id = Integer.parseInt((String) key);
                String query = (String) parse.get(key);
                queries.add(parse(query, id));
            }

        } catch (IOException | ParseException e) {
            e.printStackTrace();
        }

        queries.sort(Comparator.comparing(Query::getQueryId));
        return queries;
    }

    public void parseFromFileToCSV(String queryFile, String outputPath) throws ValidationException, SqlParseException, RelConversionException {
        List<Query> queries = parseFromFile(queryFile);

        try (PrintWriter writer = new PrintWriter(new FileWriter(outputPath + "queries.csv"))) {
            writer.println(Query.getMetaString());
            for (Query query : queries) {
                writer.println(query.toString());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void analyzeJoinRel(RelNode root) {
        class JoinRelExtractor extends RelVisitor {
            @Override
            public void visit(RelNode node, int ordinal, @Nullable RelNode parent) {
                if (node instanceof Join) {
                    System.out.println(((Join) node).getJoinType());
                    System.out.println(((Join) node).analyzeCondition().isEqui());
                    System.out.println(((Join) node).getLeft());
                }
                super.visit(node, ordinal, parent);
            }

            public void run(RelNode node) {
                go(node);
            }
        }

        new JoinRelExtractor().run(root);
    }

    private int getJoinCount(RelNode root) {
        class JoinCounter extends RelVisitor {
            int count = 0;
            @Override
            public void visit(RelNode node, int ordinal, @Nullable RelNode parent) {
                if (node instanceof Join) {
                    count ++;
                }
                super.visit(node, ordinal, parent);
            }

            int run(RelNode node) {
                go(node);
                return count;
            }
        }

        return new JoinCounter().run(root);
    }

    private int getTableCount(RelNode root) {
        class TableCounter extends RelVisitor {
            Set<String> tables = new HashSet<>();
            @Override
            public void visit(RelNode node, int ordinal, @Nullable RelNode parent) {
                if (node instanceof TableScan) {
                    this.tables.addAll(((TableScan) node).getTable().getQualifiedName());
                }
                super.visit(node, ordinal, parent);
            }

            int run(RelNode node) {
                go(node);
                return tables.size();
            }
        }

        return new TableCounter().run(root);
    }

    private int isAggregate(RelNode root) {
        class AggregateFinder extends RelVisitor {
            int flag = 0;
            @Override
            public void visit(RelNode node, int ordinal, @Nullable RelNode parent) {
                if (node instanceof Aggregate) {
                    flag = 1;
                }
                super.visit(node, ordinal, parent);
            }

            int run(RelNode node) {
                go(node);
                return flag;
            }
        }

        return new AggregateFinder().run(root);
    }

    static class QueryStats {
        int selectListSize;
        int isFilterInWhere;
        int isGroupBy;
        int isHaving;
        int isOrderBy;
        int isDistinct;

        QueryStats(int selectListSize, int isFilterInWhere, int isGroupBy, int isHaving, int isOrderBy, int isDistinct) {
            this.selectListSize = selectListSize;
            this.isFilterInWhere = isFilterInWhere;
            this.isGroupBy = isGroupBy;
            this.isHaving = isHaving;
            this.isOrderBy = isOrderBy;
            this.isDistinct = isDistinct;
        }
    }

    private QueryStats getQueryStats(String sql) throws ValidationException, SqlParseException {
        SqlNode sqlNode = this.rewriter.parseSql(sql);

        int selectListSize = ((SqlSelect) sqlNode).getSelectList().size();
        int isFilterInWhere = ((SqlSelect) sqlNode).hasWhere() ? 1 : 0;
        int isGroupBy = ((SqlSelect) sqlNode).getGroup() != null ? 1 : 0;
        int isHaving = ((SqlSelect) sqlNode).getHaving() != null ? 1 : 0;
        int isDistinct = ((SqlSelect) sqlNode).isDistinct() ? 1 : 0;
        int isOrderBy = ((SqlSelect) sqlNode).hasOrderBy() ? 1 : 0;

        return new QueryStats(selectListSize, isFilterInWhere, isGroupBy, isHaving, isOrderBy, isDistinct);
    }


    public static void main(String[] args) throws SqlParseException, ValidationException, RelConversionException {
        String sql = "SELECT COUNT(*) \n" +
                "FROM comments as c, posts as p, users as u \n" +
                "WHERE u.Id = p.OwnerUserId AND c.UserId = u.Id AND c.CreationDate>='2010-07-27 17:46:38' AND p.AnswerCount>=0 AND p.AnswerCount<=4 AND p.CommentCount>=0 AND p.CommentCount<=11 AND p.CreationDate>='2010-07-26 09:46:48' AND p.CreationDate<='2014-09-13 10:09:50' AND u.Reputation>=1 AND u.CreationDate>='2010-08-03 19:42:40' AND u.CreationDate<='2014-09-12 02:20:03'";

        SchemaPlus schema = BuildinSchema.getStatsSchema();
        QueryRewriter rewriter = new QueryRewriter(schema, List.of());
        RelNode relNode = rewriter.sqlToRelNode(sql);
        System.out.println(relNode.explain());

        QueryParser queryParser = new QueryParser(schema);
        queryParser.analyzeJoinRel(relNode);

//        System.out.println(queryParser.getTableCount(relNode));
//
//        queryParser.getQueryStats(sql);
//
//        System.out.println(queryParser.isAggregate(relNode));
//
//        List<Query> queries = queryParser.parseFromFile("./results/stats_queries.json");
//        queries.forEach(System.out::println);
//
//        queryParser.parseFromFileToCSV("./results/stats_queries.json", "./");
    }
}
