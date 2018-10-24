package com.bobocode;

import com.bobocode.util.JdbcException;
import com.bobocode.util.JdbcUtil;

import javax.sql.DataSource;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

abstract class DbInitializerTestBase {

    private static DataSource dataSource;

    static void initDataSource(DbInitializer dbInitializer) throws SQLException {
        dataSource = JdbcUtil.createDefaultInMemoryH2DataSource();
        dbInitializer.setDataSource(dataSource);
        dbInitializer.init();
    }

    void consumeStatement(Consumer<Statement> statement) throws SQLException {
        JdbcUtil.consumeStatement(dataSource, statement);
    }

    List<String> fetchValues(ResultSet resultSet, String columnName)  {
        List<String> columns = new ArrayList<>();
        try {
            while (resultSet.next()) {
                String column = getStringSafely(resultSet, columnName);
                columns.add(column);
            }
        } catch (SQLException e) {
            throw new JdbcException("Can't parse resultSet", e);
        }
        return columns;
    }

    boolean getNextSafely(ResultSet resultSet) {
        try {
            return resultSet.next();
        } catch (SQLException e) {
            throw new JdbcException("Can't execute resultSet.next()", e);
        }
    }

    String getStringSafely(ResultSet resultSet, String column)  {
        try {
            return resultSet.getString(column);
        } catch (SQLException e) {
            throw new JdbcException("Can't execute resultSet.getStringSafely() for string: " + column, e);
        }
    }

    int getIntSafely(ResultSet resultSet, String column)  {
        try {
            return resultSet.getInt(column);
        } catch (SQLException e) {
            throw new JdbcException("Can't execute resultSet.getStringSafely() for string: " + column, e);
        }
    }

    boolean getBooleanSafely(ResultSet resultSet, String column)  {
        try {
            return resultSet.getBoolean(column);
        } catch (SQLException e) {
            throw new JdbcException("Can't execute resultSet.getStringSafely() for string: " + column, e);
        }
    }
}
