package models;

public class Column {
    private int columnId;
    private int tableId;
    private String columnName;
    private int columnType;
    private int hasIndex;

    private int width;
    private int distinct;
    private double nullFrac;
    private double correlation;

    // no statistics in the first round

    public Column(int columnId, int tableId, String columnName, int columnType, int hasIndex) {
        assert columnType >= 0 && columnType <= 4;
        assert hasIndex == 0 || hasIndex == 1;

        this.columnId = columnId;
        this.tableId = tableId;
        this.columnName = columnName;
        this.columnType = columnType;
        this.hasIndex = hasIndex;
    }

    public void setColumnId(int columnId) {
        this.columnId = columnId;
    }

    public void setTableId(int tableId) {
        this.tableId = tableId;
    }

    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }

    public void setColumnType(int columnType) {
        // 0: float, 1: integer, 2: date, 3: text 4: others
        assert columnType >= 0 && columnType <= 4;
        this.columnType = columnType;
    }

    public void setHasIndex(int hasIndex) {
        assert hasIndex == 0 || hasIndex == 1;
        this.hasIndex = hasIndex;
    }

    public void setWidth(int width) {
        this.width = width;
    }

    public void setCorrelation(double correlation) {
        this.correlation = correlation;
    }

    public void setDistinct(int distinct) {
        this.distinct = distinct;
    }

    public void setNullFrac(double nullFrac) {
        this.nullFrac = nullFrac;
    }

    public int getHasIndex() {
        return hasIndex;
    }

    public int getColumnType() {
        return columnType;
    }

    public String getColumnName() {
        return columnName;
    }

    public int getColumnId() {
        return columnId;
    }

    public int getTableId() {
        return tableId;
    }

    public String getAttNames() {
        return "";
    }

    public int getWidth() {
        return width;
    }

    public double getCorrelation() {
        return correlation;
    }

    public double getNullFrac() {
        return nullFrac;
    }

    public int getDistinct() {
        return distinct;
    }

    public static String getMetaString() {
        return "columnId,tableId,columnName,columnType,hasIndex,width,distinct,nullFrac,correlation";
    }

    @Override
    public String toString() {
        String format = "%d,%d,'%s',%d,%d,%d,%d,%f,%f";
        return format.formatted(columnId, tableId, columnName, columnType, hasIndex, width, distinct, nullFrac, correlation);
    }
}
