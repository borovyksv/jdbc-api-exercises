package com.bobocode.util;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class TX_lessonDemo {
    private static DataSource dataSource;
    private final static String SQL_QUERY = "INSERT INTO alien(name) VALUES (?);";

    public static void main(String[] args) throws SQLException {
        init();
        saveSomeAlienTxt();
    }

    private static void init() {
        dataSource = JdbcUtil.createPostgresDataSource(
                "jdbc:postgresql://localhost:5432/baseone",
                "vlad", "root");
    }

    private static void saveSomeAlienTxt() throws SQLException {
        try (Connection connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
//            connection.setReadOnly(true);
            PreparedStatement insertStatement
                    = connection
                    .prepareStatement(SQL_QUERY);
            insertStatement.setString(1, "BATMAN");
            insertStatement.executeUpdate();

            connection.commit();
//            connection.rollback();
        }
    }
}