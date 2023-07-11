package testquery;

import java.util.List;

public class QueryPreprossor {
    private String sql;
    private static final List<String> ESCAPE = List.of("\t", "\b", "\n", "\r", "\f");

    public QueryPreprossor(String sql) {
        this.sql = sql;
    }

    public QueryPreprossor removeDoubleQuotation() {
        sql = sql.replaceAll("\"", "");
        return this;
    }

    public QueryPreprossor removeSemicolon() {
        sql = sql.replaceAll(";", "");
        return this;
    }

    public QueryPreprossor removeEscapeCharacter() {
        for (String e : ESCAPE) {
            sql = sql.replaceAll(e, " ");
        }
        return this;
    }

    public QueryPreprossor inOneLine() {
        sql = sql.replaceAll(System.lineSeparator(), " ");
        return this;
    }

    @Override
    public String toString() {
        return this.sql;
    }
}
