package com.bobocode;

import com.bobocode.util.FileReader;

import java.sql.SQLException;

/**
 * {@link UserProfileDbInitializer} is an API that has only one method. It allow to create a database tables to store
 * information about users and their profiles.
 */
public class UserProfileDbInitializer extends DbInitializer {
    private final static String TABLE_INITIALIZATION_SQL_FILE = "db/migration/table_initialization.sql"; // todo: see the file

    /**
     * Reads the SQL script form the file and executes it
     *
     * @throws SQLException
     */
    public void init() throws SQLException {
        String createTablesSql = FileReader.readWholeFileFromResources(TABLE_INITIALIZATION_SQL_FILE);
        createTable(createTablesSql);
    }
}
