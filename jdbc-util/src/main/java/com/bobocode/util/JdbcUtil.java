package com.bobocode.util;

import org.h2.jdbcx.JdbcDataSource;
import org.postgresql.ds.PGSimpleDataSource;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;

public class JdbcUtil {
    private static final String DEFAULT_DATABASE_NAME = "bobocode_db";
    private static final String DEFAULT_USERNAME = "bobouser";
    private static final String DEFAULT_PASSWORD = "bobodpass";

    public static DataSource createDefaultInMemoryH2DataSource() {
        String url = formatH2ImMemoryDbUrl(DEFAULT_DATABASE_NAME);
        return createInMemoryH2DataSource(url, DEFAULT_USERNAME, DEFAULT_PASSWORD);
    }

    private static DataSource createInMemoryH2DataSource(String url, String username, String pass) {
        JdbcDataSource h2DataSource = new JdbcDataSource();
        h2DataSource.setUser(username);
        h2DataSource.setPassword(pass);
        h2DataSource.setUrl(url);

        return h2DataSource;
    }

    private static String formatH2ImMemoryDbUrl(String databaseName) {
        return String.format("jdbc:h2:mem:%s;DB_CLOSE_DELAY=-1;DB_CLOSE_ON_EXIT=false;DATABASE_TO_UPPER=false;", databaseName);
    }

    public static DataSource createDefaultPostgresDataSource() {
        String url = formatPostgresDbUrl(DEFAULT_DATABASE_NAME);
        return createPostgresDataSource(url, DEFAULT_USERNAME, DEFAULT_PASSWORD);
    }

    private static DataSource createPostgresDataSource(String url, String username, String pass) {
        PGSimpleDataSource dataSource = new PGSimpleDataSource();
        dataSource.setUrl(url);
        dataSource.setUser(username);
        dataSource.setPassword(pass);
        return dataSource;
    }

    private static String formatPostgresDbUrl(String databaseName) {
        return String.format("jdbc:postgresql://localhost:5432/%s", databaseName);
    }

    public static Map<String, String> getInMemoryDbPropertiesMap() {
        return Map.of(
                "url", String.format("jdbc:h2:mem:%s", DEFAULT_DATABASE_NAME),
                "username", DEFAULT_USERNAME,
                "password", DEFAULT_PASSWORD);
    }

    public static boolean executeSafely(Statement statement, String sql) {
        try {
            return statement.execute(sql);
        } catch (SQLException e) {
            throw new JdbcException("Can't executeSafely statement", e);
        }

    }

    public static ResultSet executeQuerySafely(Statement statement, String sql) {
        try {
            return statement.executeQuery(sql);
        } catch (SQLException e) {
            throw new JdbcException("Can't executeQuerySafely with statement", e);
        }

    }

    public static void consumeStatement(DataSource dataSource, Consumer<Statement> consumer) throws SQLException {
        try (Connection connection = dataSource.getConnection();
             Statement statement = connection.createStatement()) {
            consumer.accept(statement);
        }
    }

    public static void consumeConnection(DataSource dataSource, Consumer<Connection> consumer) {
        try (Connection connection = dataSource.getConnection()) {
            consumer.accept(connection);
        } catch (SQLException e) {
            throw new JdbcException("Can't retrieve connection", e);
        }
    }

    public static <T> T applyConnection(DataSource dataSource, Function<Connection, T> function) {
        try (Connection connection = dataSource.getConnection()) {
            return function.apply(connection);
        } catch (SQLException e) {
            throw new JdbcException("Can't retrieve connection", e);
        }
    }
}
