package com.bobocode;

import javax.sql.DataSource;
import java.sql.SQLException;

import static com.bobocode.util.JdbcUtil.consumeStatement;
import static com.bobocode.util.JdbcUtil.executeSafely;

abstract class DbInitializer {
    private DataSource dataSource;

    abstract void init() throws SQLException;

    void setDataSource(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    private DataSource getDataSource() {
        return dataSource;
    }

    public void createTable(String createTablesSql) throws SQLException {
        consumeStatement(getDataSource(), statement ->
                executeSafely(statement, createTablesSql));
    }
}
