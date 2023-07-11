package models;

public class Table {
    private int tableId;
    private String tableName;
    private int rowNum;
    private int colNum;

    public Table(int id, String tableName) {
        this.tableId = id;
        this.tableName = tableName;
    }

    public void setColNum(int colNum) {
        this.colNum = colNum;
    }

    public void setRowNum(int rowNum) {
        this.rowNum = rowNum;
    }

    public int getTableId() {
        return tableId;
    }

    public int getColNum() {
        return colNum;
    }

    public int getRowNum() {
        return rowNum;
    }

    public String getTableName() {
        return tableName;
    }

    public static String getMetaString() {
        return "tableId,tableName,rowNum,colNum";
    }

    @Override
    public String toString() {
        return "%d,'%s',%d,%d".formatted(tableId, tableName, rowNum, colNum);
    }
}
