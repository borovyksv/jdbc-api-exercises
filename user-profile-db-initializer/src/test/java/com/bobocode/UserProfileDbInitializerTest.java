package com.bobocode;

import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import static com.bobocode.util.JdbcUtil.executeQuerySafely;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;


public class UserProfileDbInitializerTest extends DbInitializerTestBase {

    @BeforeClass
    public static void init() throws SQLException {
        initDataSource(new UserProfileDbInitializer());
    }

    @Test
    public void testTablesHaveCorrectNames() throws SQLException {
        consumeStatement(statement -> {
            ResultSet resultSet = executeQuerySafely(statement, "SHOW TABLES");
            List<String> tableNames = fetchValues(resultSet, "table_name");

            assertThat(tableNames, containsInAnyOrder("users", "profiles"));
        });
    }

    @Test
    public void testUsersTablesHasPrimaryKey() throws SQLException {
        consumeStatement(statement -> {
            ResultSet resultSet = executeQuerySafely(statement, "SELECT * FROM INFORMATION_SCHEMA.CONSTRAINTS" +
                    " WHERE table_name = 'users' AND constraint_type = 'PRIMARY_KEY';");
            boolean resultIsNotEmpty = getNextSafely(resultSet);

            assertThat(resultIsNotEmpty, is(true));
        });
    }

    @Test
    public void testUsersTablePrimaryKeyHasCorrectName() throws SQLException {
        consumeStatement(statement -> {
            ResultSet resultSet = executeQuerySafely(statement, "SELECT * FROM INFORMATION_SCHEMA.CONSTRAINTS" +
                    " WHERE table_name = 'users' AND constraint_type = 'PRIMARY_KEY';");
            getNextSafely(resultSet);
            String pkConstraintName = getStringSafely(resultSet,"constraint_name");

            assertThat(pkConstraintName, equalTo("users_PK"));
        });
    }

    @Test
    public void testUsersTablePrimaryKeyBasedOnIdField() throws SQLException {
        consumeStatement(statement -> {
            ResultSet resultSet = executeQuerySafely(statement, "SELECT * FROM INFORMATION_SCHEMA.CONSTRAINTS" +
                    " WHERE table_name = 'users' AND constraint_type = 'PRIMARY_KEY';");
            getNextSafely(resultSet);
            String pkColumn = getStringSafely(resultSet,"column_list");

            assertThat("id", equalTo(pkColumn));
        });
    }

    @Test
    public void testUsersTableHasCorrectAlternativeKey() throws SQLException {
        consumeStatement(statement -> {
            ResultSet resultSet = executeQuerySafely(statement, "SELECT * FROM INFORMATION_SCHEMA.CONSTRAINTS" +
                    " WHERE table_name = 'users' AND constraint_type = 'UNIQUE';");
            getNextSafely(resultSet);
            String uniqueConstraintName = getStringSafely(resultSet,"constraint_name");
            String uniqueConstraintColumn = getStringSafely(resultSet,"column_list");

            assertThat(uniqueConstraintName, equalTo("users_email_AK"));
            assertThat(uniqueConstraintColumn, equalTo("email"));
        });
    }

    @Test
    public void testUsersTableHasAllRequiredColumns() throws SQLException {
        consumeStatement(statement -> {
            ResultSet resultSet = executeQuerySafely(statement, "SELECT * FROM INFORMATION_SCHEMA.COLUMNS" +
                    " WHERE table_name = 'users';");
            List<String> columns = fetchValues(resultSet, "column_name");

            assertThat(columns.size(), equalTo(5));
            assertThat(columns, containsInAnyOrder("id", "email", "first_name", "last_name", "birthday"));
        });
    }

    @Test
    public void testUsersTableRequiredColumnsHaveHaveNotNullConstraint() throws SQLException {
        consumeStatement(statement -> {
            ResultSet resultSet = executeQuerySafely(statement, "SELECT * FROM INFORMATION_SCHEMA.COLUMNS" +
                    " WHERE table_name = 'users' AND nullable = false;");
            List<String> notNullColumns = fetchValues(resultSet, "column_name");

            assertThat(notNullColumns.size(), is(5));
            assertThat(notNullColumns, containsInAnyOrder("id", "email", "first_name", "last_name", "birthday"));
        });
    }

    @Test
    public void testUserIdTypeIsBigint() throws SQLException {
        consumeStatement(statement -> {
            ResultSet resultSet = executeQuerySafely(statement, "SELECT * FROM INFORMATION_SCHEMA.COLUMNS" +
                    " WHERE table_name = 'users' AND column_name = 'id';");
            getNextSafely(resultSet);
            String idTypeName = getStringSafely(resultSet,"type_name");

            assertThat(idTypeName, is("BIGINT"));
        });
    }

    @Test
    public void testUsersTableStringColumnsHaveCorrectTypeAndLength() throws SQLException {
        consumeStatement(statement -> {
            ResultSet resultSet = executeQuerySafely(statement, "SELECT column_name FROM INFORMATION_SCHEMA.COLUMNS" +
                    " WHERE table_name = 'users' AND type_name = 'VARCHAR' AND character_maximum_length = 255;");
            List<String> stringColumns = fetchValues(resultSet, "column_name");

            assertThat(stringColumns.size(), is(3));
            assertThat(stringColumns, containsInAnyOrder("email", "first_name", "last_name"));
        });
    }

    @Test
    public void testUserBirthdayTypeIsDate() throws SQLException {
        consumeStatement(statement -> {
            ResultSet resultSet = executeQuerySafely(statement, "SELECT * FROM INFORMATION_SCHEMA.COLUMNS" +
                    " WHERE table_name = 'users' AND column_name = 'birthday';");
            getNextSafely(resultSet);
            String idTypeName = getStringSafely(resultSet,"type_name");

            assertThat(idTypeName, is("DATE"));
        });
    }

    // table sale_group test

    @Test
    public void testProfilesTablesHasPrimaryKey() throws SQLException {
        consumeStatement(statement -> {
            ResultSet resultSet = executeQuerySafely(statement, "SELECT * FROM INFORMATION_SCHEMA.CONSTRAINTS" +
                    " WHERE table_name = 'profiles' AND constraint_type = 'PRIMARY_KEY';");
            boolean resultIsNotEmpty = getNextSafely(resultSet);

            assertThat(resultIsNotEmpty, is(true));
        });
    }

    @Test
    public void testProfilesTablePrimaryKeyHasCorrectName() throws SQLException {
        consumeStatement(statement -> {
            ResultSet resultSet = executeQuerySafely(statement, "SELECT * FROM INFORMATION_SCHEMA.CONSTRAINTS" +
                    " WHERE table_name = 'profiles' AND constraint_type = 'PRIMARY_KEY';");
            getNextSafely(resultSet);
            String pkConstraintName = getStringSafely(resultSet,"constraint_name");

            assertThat(pkConstraintName, equalTo("profiles_PK"));
        });
    }

    @Test
    public void testProfilesTablePrimaryKeyBasedOnForeignKeyColumn() throws SQLException {
        consumeStatement(statement -> {
            ResultSet resultSet = executeQuerySafely(statement, "SELECT * FROM INFORMATION_SCHEMA.CONSTRAINTS" +
                    " WHERE table_name = 'profiles' AND constraint_type = 'PRIMARY_KEY';");
            getNextSafely(resultSet);
            String pkColumn = getStringSafely(resultSet,"column_list");

            assertThat("user_id", equalTo(pkColumn));
        });
    }

    @Test
    public void testProfilesTableHasAllRequiredColumns() throws SQLException {
        consumeStatement(statement -> {
            ResultSet resultSet = executeQuerySafely(statement, "SELECT * FROM INFORMATION_SCHEMA.COLUMNS" +
                    " WHERE table_name = 'profiles';");
            List<String> columns = fetchValues(resultSet, "column_name");

            assertThat(columns.size(), equalTo(5));
            assertThat(columns, containsInAnyOrder("user_id", "job_position", "company", "education", "city"));
        });
    }

    @Test
    public void testProfilesGroupIdTypeIsBigint() throws SQLException {
        consumeStatement(statement -> {
            ResultSet resultSet = executeQuerySafely(statement, "SELECT * FROM INFORMATION_SCHEMA.COLUMNS" +
                    " WHERE table_name = 'profiles' AND column_name = 'user_id';");
            getNextSafely(resultSet);
            String idTypeName = getStringSafely(resultSet,"type_name");

            assertThat(idTypeName, is("BIGINT"));
        });
    }

    @Test
    public void testProfilesTableStringColumnsHaveCorrectTypeAndLength() throws SQLException {
        consumeStatement(statement -> {
            ResultSet resultSet = executeQuerySafely(statement, "SELECT column_name FROM INFORMATION_SCHEMA.COLUMNS" +
                    " WHERE table_name = 'profiles' AND type_name = 'VARCHAR' AND character_maximum_length = 255;");
            List<String> stringColumns = fetchValues(resultSet, "column_name");

            assertThat(stringColumns.size(), is(4));
            assertThat(stringColumns, containsInAnyOrder("job_position", "company", "education", "city"));
        });
    }

    @Test
    public void testProfilesHasForeignKeyToUsers() throws SQLException {
        consumeStatement(statement -> {
            ResultSet resultSet = executeQuerySafely(statement, "SELECT * FROM INFORMATION_SCHEMA.CONSTRAINTS" +
                    " WHERE table_name = 'profiles' AND constraint_type = 'REFERENTIAL' AND column_list = 'user_id';");
            boolean resultIsNotEmpty = getNextSafely(resultSet);

            assertThat(resultIsNotEmpty, is(true));
        });
    }

    @Test
    public void testProfilesForeignKeyToUsersHasCorrectName() throws SQLException {
        consumeStatement(statement -> {
            ResultSet resultSet = executeQuerySafely(statement, "SELECT * FROM INFORMATION_SCHEMA.CONSTRAINTS" +
                    " WHERE table_name = 'profiles' AND constraint_type = 'REFERENTIAL' AND column_list = 'user_id';");
            getNextSafely(resultSet);
            String fkConstraintName = getStringSafely(resultSet,"constraint_name");

            assertThat(fkConstraintName, equalTo("profiles_users_FK"));
        });
    }
}
