package models;

public class Query {
    private int queryId;
    private String rawQuery;
    private int numTablesUsed;
    private int selectListSize;
    private int isAggregate;
    private int joinCount;
    private int isFilterInWhere;
    private int isGroupBy;
    private int isHaving;
    private int isOrderBy;
    private int isDistinct;

    public Query(int queryId, String rawQuery, int numTablesUsed, int selectListSize, int isAggregate,
                 int joinCount, int isFilterInWhere, int isGroupBy, int isHaving, int isOrderBy, int isDistinct) {
        assert isAggregate == 0 || isAggregate == 1;
        assert joinCount >= 0;
        assert isFilterInWhere == 0 || isFilterInWhere == 1;
        assert isGroupBy == 0 || isGroupBy == 1;
        assert isHaving == 0 || isHaving == 1;
        assert isOrderBy == 0 || isOrderBy == 1;
        assert isDistinct == 0 || isDistinct == 1;

        this.queryId = queryId;
        this.rawQuery = rawQuery;
        this.numTablesUsed = numTablesUsed;
        this.selectListSize = selectListSize;
        this.isAggregate = isAggregate;
        this.joinCount = joinCount;
        this.isFilterInWhere = isFilterInWhere;
        this.isGroupBy = isGroupBy;
        this.isHaving = isHaving;
        this.isOrderBy = isOrderBy;
        this.isDistinct = isDistinct;
    }

    public int getQueryId() {
        return queryId;
    }

    public String getRawQuery() {
        return rawQuery;
    }

    public int getNumTablesUsed() {
        return numTablesUsed;
    }

    public int selectListSize() {
        return selectListSize;
    }

    public int getIsAggregate() {
        return isAggregate;
    }

    public int getJoinCount() {
        return joinCount;
    }

    public int getIsFilterInWhere() {
        return isFilterInWhere;
    }

    public int getIsGroupBy() {
        return isGroupBy;
    }

    public int getIsHaving() {
        return isHaving;
    }

    public int getIsOrderBy() {
        return isOrderBy;
    }

    public int getIsDistinct() {
        return isDistinct;
    }

    public static String getMetaString() {
        return "queryId,numTablesUsed,selectListSize,isAggregate,joinCount,isFilterInWhere," +
                "isGroupBy,isHaving,isOrderBy,isDistinct";
    }

    @Override
    public String toString() {
        return "%d,%d,%d,%d,%d,%d,%d,%d,%d,%d".formatted(
                queryId, numTablesUsed, selectListSize, isAggregate, joinCount, isFilterInWhere,
                isGroupBy, isHaving, isOrderBy, isDistinct
        );
    }
}
