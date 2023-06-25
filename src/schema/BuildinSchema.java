package schema;

import org.apache.calcite.config.Lex;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.server.DdlExecutorImpl;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.ddl.SqlDdlNodes;
import org.apache.calcite.sql.parser.SqlAbstractParserImpl;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParserImplFactory;
import org.apache.calcite.sql.parser.ddl.SqlDdlParserImpl;
import org.apache.calcite.sql.parser.impl.SqlParserImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.util.SqlBasicVisitor;
import org.apache.calcite.sql.util.SqlVisitor;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.util.Pair;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

public class BuildinSchema {
    public static SchemaPlus getTpchSchema() {
        SchemaPlus rootSchema = Frameworks.createRootSchema(true);

        SimpleTable customer = SimpleTable.newTable(
                "customer",
                List.of(
                        Pair.of("c_custkey", SqlTypeName.INTEGER),
                        Pair.of("c_name", SqlTypeName.VARCHAR),
                        Pair.of("c_address", SqlTypeName.VARCHAR),
                        Pair.of("c_nationkey", SqlTypeName.INTEGER),
                        Pair.of("c_phone", SqlTypeName.CHAR),
                        Pair.of("c_acctbal", SqlTypeName.DECIMAL),
                        Pair.of("c_mktsegment", SqlTypeName.CHAR),
                        Pair.of("c_comment", SqlTypeName.VARCHAR),
                        Pair.of("c_null", SqlTypeName.VARCHAR)
                )
        );
        SimpleTable lineitem = SimpleTable.newTable(
                "lineitem",
                List.of(
                        Pair.of("l_orderkey", SqlTypeName.INTEGER),
                        Pair.of("l_partkey", SqlTypeName.INTEGER),
                        Pair.of("l_suppkey", SqlTypeName.INTEGER),
                        Pair.of("l_linenumber", SqlTypeName.INTEGER),
                        Pair.of("l_quantity", SqlTypeName.DECIMAL),
                        Pair.of("l_extendedprice", SqlTypeName.DECIMAL),
                        Pair.of("l_discount", SqlTypeName.DECIMAL),
                        Pair.of("l_tax", SqlTypeName.DECIMAL),
                        Pair.of("l_returnflag", SqlTypeName.CHAR),
                        Pair.of("l_linestatus", SqlTypeName.CHAR),
                        Pair.of("l_shipdate", SqlTypeName.DATE),
                        Pair.of("l_commitdate", SqlTypeName.DATE),
                        Pair.of("l_receiptdate", SqlTypeName.DATE),
                        Pair.of("l_shipinstruct", SqlTypeName.CHAR),
                        Pair.of("l_shipmode", SqlTypeName.CHAR),
                        Pair.of("l_comment", SqlTypeName.VARCHAR),
                        Pair.of("l_null", SqlTypeName.VARCHAR)
                )
        );
        SimpleTable nation = SimpleTable.newTable(
                "nation",
                List.of(
                        Pair.of("n_nationkey", SqlTypeName.INTEGER),
                        Pair.of("n_name", SqlTypeName.CHAR),
                        Pair.of("n_regionkey", SqlTypeName.INTEGER),
                        Pair.of("n_comment", SqlTypeName.VARCHAR),
                        Pair.of("n_null", SqlTypeName.VARCHAR)
                )
        );
        SimpleTable orders = SimpleTable.newTable(
                "orders",
                List.of(
                        Pair.of("o_orderkey", SqlTypeName.INTEGER),
                        Pair.of("o_custkey", SqlTypeName.INTEGER),
                        Pair.of("o_orderstatus", SqlTypeName.CHAR),
                        Pair.of("o_totalprice", SqlTypeName.DECIMAL),
                        Pair.of("o_orderdate", SqlTypeName.DATE),
                        Pair.of("o_orderpriority", SqlTypeName.CHAR),
                        Pair.of("o_clerk", SqlTypeName.CHAR),
                        Pair.of("o_shippriority", SqlTypeName.INTEGER),
                        Pair.of("o_comment", SqlTypeName.VARCHAR),
                        Pair.of("o_null", SqlTypeName.VARCHAR)
                )
        );
        SimpleTable part = SimpleTable.newTable(
                "part",
                List.of(
                        Pair.of("p_partkey", SqlTypeName.INTEGER),
                        Pair.of("p_name", SqlTypeName.VARCHAR),
                        Pair.of("p_mfgr", SqlTypeName.CHAR),
                        Pair.of("p_brand", SqlTypeName.CHAR),
                        Pair.of("p_type", SqlTypeName.VARCHAR),
                        Pair.of("p_size", SqlTypeName.INTEGER),
                        Pair.of("p_container", SqlTypeName.CHAR),
                        Pair.of("p_retailprice", SqlTypeName.DECIMAL),
                        Pair.of("p_comment", SqlTypeName.VARCHAR),
                        Pair.of("p_null", SqlTypeName.VARCHAR)
                )
        );
        SimpleTable partsupp = SimpleTable.newTable(
                "partsupp",
                List.of(
                        Pair.of("ps_partkey", SqlTypeName.INTEGER),
                        Pair.of("ps_suppkey", SqlTypeName.INTEGER),
                        Pair.of("ps_availqty", SqlTypeName.INTEGER),
                        Pair.of("ps_supplycost", SqlTypeName.DECIMAL),
                        Pair.of("ps_comment", SqlTypeName.VARCHAR),
                        Pair.of("ps_null", SqlTypeName.VARCHAR)
                )
        );
        SimpleTable region = SimpleTable.newTable(
                "region",
                List.of(
                        Pair.of("r_regionkey", SqlTypeName.INTEGER),
                        Pair.of("r_name", SqlTypeName.CHAR),
                        Pair.of("r_comment", SqlTypeName.VARCHAR),
                        Pair.of("r_null", SqlTypeName.VARCHAR)
                )
        );
        SimpleTable supplier = SimpleTable.newTable(
                "supplier",
                List.of(
                        Pair.of("s_suppkey", SqlTypeName.INTEGER),
                        Pair.of("s_name", SqlTypeName.CHAR),
                        Pair.of("s_address", SqlTypeName.VARCHAR),
                        Pair.of("s_nationkey", SqlTypeName.INTEGER),
                        Pair.of("s_phone", SqlTypeName.CHAR),
                        Pair.of("s_acctbal", SqlTypeName.DECIMAL),
                        Pair.of("s_comment", SqlTypeName.VARCHAR),
                        Pair.of("s_null", SqlTypeName.VARCHAR)
                )
        );

        rootSchema.add(customer.getTableName(), customer);
        rootSchema.add(lineitem.getTableName(), lineitem);
        rootSchema.add(nation.getTableName(), nation);
        rootSchema.add(orders.getTableName(), orders);
        rootSchema.add(part.getTableName(), part);
        rootSchema.add(partsupp.getTableName(), partsupp);
        rootSchema.add(region.getTableName(), region);
        rootSchema.add(supplier.getTableName(), supplier);

        return rootSchema;
    }

    public static SchemaPlus getStatsSchema() {
        SchemaPlus rootSchema = Frameworks.createRootSchema(true);

        SimpleTable badges = SimpleTable.newTable("badges",
                List.of(
                        Pair.of("Id", SqlTypeName.INTEGER),
                        Pair.of("UserId", SqlTypeName.INTEGER),
                        Pair.of("Name", SqlTypeName.VARCHAR),
                        Pair.of("Date", SqlTypeName.DATE)
                )
        );
        SimpleTable comments = SimpleTable.newTable("comments",
                List.of(
                        Pair.of("Id", SqlTypeName.INTEGER),
                        Pair.of("PostId", SqlTypeName.INTEGER),
                        Pair.of("Score", SqlTypeName.INTEGER),
                        Pair.of("Text", SqlTypeName.VARCHAR),
                        Pair.of("CreationDate", SqlTypeName.INTEGER),
                        Pair.of("UserId", SqlTypeName.INTEGER),
                        Pair.of("UserDisplayName", SqlTypeName.INTEGER)
                )
        );
        SimpleTable postHistory = SimpleTable.newTable("postHistory",
                List.of(
                        Pair.of("Id", SqlTypeName.INTEGER),
                        Pair.of("PostHistoryTypeId", SqlTypeName.INTEGER),
                        Pair.of("PostId", SqlTypeName.INTEGER),
                        Pair.of("RevisionGUID", SqlTypeName.VARCHAR),
                        Pair.of("CreationDate", SqlTypeName.DATE),
                        Pair.of("UserId", SqlTypeName.INTEGER),
                        Pair.of("Text", SqlTypeName.VARCHAR),
                        Pair.of("Comment", SqlTypeName.VARCHAR),
                        Pair.of("UserDisplayName", SqlTypeName.VARCHAR)
                )
        );
        SimpleTable postLinks = SimpleTable.newTable("postLinks",
                List.of(
                        Pair.of("Id", SqlTypeName.INTEGER),
                        Pair.of("CreationDate", SqlTypeName.DATE),
                        Pair.of("PostId", SqlTypeName.INTEGER),
                        Pair.of("RelatedPostId", SqlTypeName.INTEGER),
                        Pair.of("LinkTypeId", SqlTypeName.INTEGER)
                )
        );
        SimpleTable posts = SimpleTable.newTable("posts",
                List.of(
                        Pair.of("Id", SqlTypeName.INTEGER),
                        Pair.of("PostTypeId", SqlTypeName.INTEGER),
                        Pair.of("AcceptedAnswerId", SqlTypeName.INTEGER),
                        Pair.of("CreationDate", SqlTypeName.DATE),
                        Pair.of("Score", SqlTypeName.INTEGER),
                        Pair.of("ViewCount", SqlTypeName.INTEGER),
                        Pair.of("Body", SqlTypeName.VARCHAR),
                        Pair.of("OwnerUserId", SqlTypeName.INTEGER),
                        Pair.of("LasActivityDate", SqlTypeName.DATE),
                        Pair.of("Title", SqlTypeName.VARCHAR),
                        Pair.of("Tags", SqlTypeName.VARCHAR),
                        Pair.of("AnswerCount", SqlTypeName.INTEGER),
                        Pair.of("CommentCount", SqlTypeName.INTEGER),
                        Pair.of("FavoriteCount", SqlTypeName.INTEGER),
                        Pair.of("LastEditorUserId", SqlTypeName.INTEGER),
                        Pair.of("LastEditDate", SqlTypeName.DATE),
                        Pair.of("CommunityOwnedDate", SqlTypeName.DATE),
                        Pair.of("ParentId", SqlTypeName.INTEGER),
                        Pair.of("ClosedDate", SqlTypeName.DATE),
                        Pair.of("OwnerDisplayName", SqlTypeName.VARCHAR),
                        Pair.of("LastEditorDisplayName", SqlTypeName.VARCHAR)
                )
        );
        SimpleTable tags = SimpleTable.newTable("tags",
                List.of(
                        Pair.of("Id", SqlTypeName.INTEGER),
                        Pair.of("TagName", SqlTypeName.VARCHAR),
                        Pair.of("Count", SqlTypeName.INTEGER),
                        Pair.of("ExcerptPostId", SqlTypeName.INTEGER),
                        Pair.of("WikiPostId", SqlTypeName.INTEGER)
                )
        );
        SimpleTable users = SimpleTable.newTable("users",
                List.of(
                        Pair.of("Id", SqlTypeName.INTEGER),
                        Pair.of("Reputation", SqlTypeName.INTEGER),
                        Pair.of("CreationDate", SqlTypeName.DATE),
                        Pair.of("DisplayName", SqlTypeName.VARCHAR),
                        Pair.of("LastAccessDate", SqlTypeName.DATE),
                        Pair.of("WebsiteUrl", SqlTypeName.VARCHAR),
                        Pair.of("Location", SqlTypeName.VARCHAR),
                        Pair.of("AboutMe", SqlTypeName.VARCHAR),
                        Pair.of("Views", SqlTypeName.INTEGER),
                        Pair.of("UpVotes", SqlTypeName.INTEGER),
                        Pair.of("DownVotes", SqlTypeName.INTEGER),
                        Pair.of("AccountId", SqlTypeName.INTEGER),
                        Pair.of("Age", SqlTypeName.INTEGER),
                        Pair.of("ProfileImageUrl", SqlTypeName.VARCHAR)
                )
        );
        SimpleTable votes = SimpleTable.newTable("votes",
                List.of(
                        Pair.of("Id", SqlTypeName.INTEGER),
                        Pair.of("PostId", SqlTypeName.INTEGER),
                        Pair.of("VoteTypeId", SqlTypeName.INTEGER),
                        Pair.of("CreationDate", SqlTypeName.DATE),
                        Pair.of("UserId", SqlTypeName.INTEGER),
                        Pair.of("BountyAmount", SqlTypeName.INTEGER)
                )
        );

        rootSchema.add(badges.getTableName(), badges);
        rootSchema.add(comments.getTableName(), comments);
        rootSchema.add(postHistory.getTableName(), postHistory);
        rootSchema.add(postLinks.getTableName(), postLinks);
        rootSchema.add(posts.getTableName(), posts);
        rootSchema.add(tags.getTableName(), tags);
        rootSchema.add(users.getTableName(), users);
        rootSchema.add(votes.getTableName(), votes);

        return rootSchema;
    }

    public static SchemaPlus getImdbSchema() throws SqlParseException {
        String path = "src/schema/imdb_schema.sql";
        return readSchemaFromSql(path);
    }

    public static SchemaPlus readSchemaFromSql(String path) throws SqlParseException {
        SchemaPlus rootSchema = Frameworks.createRootSchema(true);

        String ddl = null;
        try (BufferedReader reader = new BufferedReader(new FileReader(path))) {
            ddl = String.join(" ", reader.lines().toList());
        } catch (IOException e) {
            e.printStackTrace();
        }

        SqlParser.Config config = SqlParser.config()
                .withParserFactory(SqlDdlParserImpl.FACTORY)
                .withLex(Lex.ORACLE)
                .withCaseSensitive(false);

        if (ddl != null) {
            String[] split = ddl.replaceAll("PRIMARY KEY", "").split(";");

            for (String ddlSplit : split) {
                SqlParser parser = SqlParser.create(ddlSplit, config);
                SqlNode sqlNode = parser.parseStmt();

                DDLVisitor visitor = new DDLVisitor();
                sqlNode.accept(visitor);

                List<Pair<String, SqlTypeName>> columns = new ArrayList<>();
                for (String key : visitor.columnMap.keySet()) {
                    columns.add(Pair.of(key, visitor.columnMap.get(key)));
                }

                SimpleTable table = SimpleTable.newTable(visitor.tableName, columns);
                rootSchema.add(table.getTableName(), table);
            }
        }

        return rootSchema;
    }

    static class DDLVisitor extends SqlBasicVisitor<Object> {
        String tableName;
        Map<String, SqlTypeName> columnMap;

        DDLVisitor() {
            super();
            tableName = "";
            columnMap = new HashMap<>();
        }

        @Override
        public Object visit(SqlCall call) {
            if (call.getOperator().kind == SqlKind.CREATE_TABLE) {
                this.tableName = call.getOperandList().get(0).toString();
            }
            if (call.getOperator().kind == SqlKind.COLUMN_DECL) {
                String columnName = call.getOperandList().get(0).toString();
                String typeName = call.getOperandList().get(1).toString();
                this.columnMap.put(columnName, parseType(typeName));
            }
            return call.getOperator().acceptCall(this, call);
        }

        SqlTypeName parseType(String typeName) {
            typeName = typeName.toLowerCase(Locale.ROOT);

            // INTEGER
            if (typeName.contains("integer")) {
                return SqlTypeName.INTEGER;
            }
            //  VARCHAR
            if (typeName.contains("text") || typeName.contains("varchar")) {
                return SqlTypeName.VARCHAR;
            }
            // DATE
            if (typeName.contains("date")) {
                return SqlTypeName.DATE;
            }

            return null;
        }
    }
}
