package models;

public class Plan {
    private int planId;
    private int queryId;
    private double estCost;
    private int estRows;
    private double actualTime;
    private int actualRows;

    public Plan(int planId, int queryId, double estCost, int estRows, double actualTime, int actualRows) {
        this.planId = planId;
        this.queryId = queryId;
        this.estCost = estCost;
        this.estRows = estRows;
        this.actualTime = actualTime;
        this.actualRows = actualRows;
    }

    public void setPlanId(int planId) {
        this.planId = planId;
    }

    public void setQueryId(int queryId) {
        this.queryId = queryId;
    }

    public void setActualRows(int actualRows) {
        this.actualRows = actualRows;
    }

    public void setActualTime(double actualTime) {
        this.actualTime = actualTime;
    }

    public void setEstCost(double estCost) {
        this.estCost = estCost;
    }

    public void setEstRows(int estRows) {
        this.estRows = estRows;
    }

    public int getPlanId() {
        return planId;
    }

    public int getQueryId() {
        return queryId;
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

    public static String getMetaString() {
        return "planId,queryId,estCost,estRows,actualTime,actualRows";
    }

    @Override
    public String toString() {
        return "%d,%d,%f,%d,%f,%d".formatted(planId, queryId, estCost, estRows, actualTime, actualRows);
    }
}
