package models;

import java.util.Map;
import java.util.Set;

public class Operator {
    private int operatorId;
    private int tableId;
    private int planId;
    private int leftChildId;
    private int rightChildId;
    private String operatorType;
    private double estCost;
    private int estRows;
    private double actualTime;
    private int actualRows;
    private int actualLoops;
    private int hitBlocks;
    private int readBlocks;
    public static final Set<String> OPERATOR_SET = Set.of(
            "BitmapAnd", "BitmapOr", "Gather", "Gather Merge", "Nested Loop", "Merge Join", "Hash Join",
            "Seq Scan", "Index Scan", "Index Only Scan", "Bitmap Index Scan", "Bitmap Heap Scan", "Subquery Scan",
            "Materialize", "Memoize", "Sort", "Incremental Sort", "Group", "Aggregate", "GroupAggregate",
            "HashAggregate", "MixedAggregate", "WindowAgg", "Unique", "Limit", "Hash"
    );

    public Operator(int operatorId, int tableId, int planId, int leftChildId, int rightChildId,
                    String operatorType, double estCost, int estRows, double actualTime, int actualRows,
                    int actualLoops, int hitBlocks, int readBlocks) {
        assert OPERATOR_SET.contains(operatorType);

        this.operatorId = operatorId;
        this.tableId = tableId;
        this.planId = planId;
        this.leftChildId = leftChildId;
        this.rightChildId = rightChildId;
        this.operatorType = operatorType;
        this.estCost = estCost;
        this.estRows = estRows;
        this.actualTime = actualTime;
        this.actualRows = actualRows;
        this.actualLoops = actualLoops;
        this.hitBlocks = hitBlocks;
        this.readBlocks = readBlocks;
    }

    public int getOperatorId() {
        return operatorId;
    }

    public int getTableId() {
        return tableId;
    }

    public int getPlanId() {
        return planId;
    }

    public int getLeftChildId() {
        return leftChildId;
    }

    public int getRightChildId() {
        return rightChildId;
    }

    public String getOperatorType() {
        return operatorType;
    }

    public double getEstCost() {
        return estCost;
    }

    public int getEstRows() {
        return estRows;
    }

    public double getActualTime() {
        return actualTime;
    }

    public int getActualRows() {
        return actualRows;
    }

    public int getActualLoops() {
        return actualLoops;
    }

    public int getHitBlocks() {
        return hitBlocks;
    }

    public int getReadBlocks() {
        return readBlocks;
    }

    public static String getMetaString() {
        return "operatorId,tableId,planId,leftChildId,rightChildId,operatorType,estCost,estRows," +
                "actualTime,actualRows,actualLoops,hitBlocks,readBlocks";
    }

    @Override
    public String toString() {
        return "%d,%d,%d,%d,%d,'%s',%f,%d,%f,%d,%d,%d,%d".formatted(
                operatorId, tableId, planId, leftChildId, rightChildId,
                operatorType, estCost, estRows, actualTime, actualRows, actualLoops,
                hitBlocks, readBlocks
        );
    }
}
