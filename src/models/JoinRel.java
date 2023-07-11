package models;

public class JoinRel {
    private final int joinRelId;
    private final int queryId;
    private final int joinType;
    private final int leftTableId;
    private final int rightTableId;
    // simplify join condition
    private final int isEqualJoin;

    public JoinRel(int joinRelId, int queryId, int joinType, int leftTableId, int rightTableId, int isEqualJoin) {
        // 0: inner join 1: outer join
        assert joinType == 0 || joinType == 1;
        assert isEqualJoin == 0 || isEqualJoin == 1;

        this.joinRelId = joinRelId;
        this.queryId = queryId;
        this.joinType = joinType;
        this.leftTableId = leftTableId;
        this.rightTableId = rightTableId;
        this.isEqualJoin = isEqualJoin;
    }

    public int getJoinRelId() {
        return joinRelId;
    }

    public int getQueryId() {
        return queryId;
    }

    public int getJoinType() {
        return joinType;
    }

    public int getLeftTableId() {
        return leftTableId;
    }

    public int getRightTableId() {
        return rightTableId;
    }

    public int getIsEqualJoin() {
        return isEqualJoin;
    }
}
