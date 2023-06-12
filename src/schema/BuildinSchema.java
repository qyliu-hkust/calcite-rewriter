package schema;

import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.util.Pair;

import java.util.List;

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
}
