package schema;

import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.rel.type.*;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Pair;

import java.sql.Array;
import java.util.ArrayList;
import java.util.List;


public class SimpleTable extends AbstractTable implements ScannableTable {

    private final String tableName;
    private final List<String> fieldNames;
    private final List<SqlTypeName> filedTypes;

    private SimpleTable(String tableName, List<String> fieldNames, List<SqlTypeName> filedTypes) {
        this.tableName = tableName;
        assert fieldNames.size() == filedTypes.size();
        this.fieldNames = fieldNames;
        this.filedTypes = filedTypes;
    }

    public static SimpleTable newTable(String tableName, List<Pair<String, SqlTypeName>> fieldPairs) {
        List<String> fieldNames = new ArrayList<>(fieldPairs.size());
        List<SqlTypeName> fieldTypes = new ArrayList<>(fieldPairs.size());
        for (Pair<String, SqlTypeName> p : fieldPairs) {
            fieldNames.add(p.left);
            fieldTypes.add(p.right);
        }
        return new SimpleTable(tableName, fieldNames, fieldTypes);
    }

    public String getTableName() {
        return this.tableName;
    }


    @Override
    public Enumerable<Object[]> scan(DataContext dataContext) {
        throw new UnsupportedOperationException("Not Implemented.");
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory relDataTypeFactory) {
        int size = this.filedTypes.size();
        List<RelDataTypeField> fields = new ArrayList<>(size);
        for (int i=0; i<size; ++i) {
            RelDataType sqlType = relDataTypeFactory.createSqlType(this.filedTypes.get(i));
            RelDataTypeField field = new RelDataTypeFieldImpl(this.fieldNames.get(i), i, sqlType);
            fields.add(field);
        }
        return new RelRecordType(StructKind.PEEK_FIELDS, fields);
    }
}
