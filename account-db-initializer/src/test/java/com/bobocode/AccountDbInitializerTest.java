package com.bobocode;

import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import static com.bobocode.util.JdbcUtil.executeQuerySafely;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(JUnit4.class)
public class AccountDbInitializerTest extends DbInitializerTestBase {

    @BeforeClass
    public static void init() throws SQLException {
        initDataSource(new AccountDbInitializer());
    }

    @Test
    public void testTableHasCorrectName() throws SQLException {
        consumeStatement(statement -> {
            ResultSet resultSet = executeQuerySafely(statement, "SHOW TABLES");
            getNextSafely(resultSet);
            String tableName = getStringSafely(resultSet, "table_name");

            assertEquals("account", tableName);
        });
    }

    @Test
    public void testTableHasPrimaryKey() throws SQLException {
        consumeStatement(statement -> {
            ResultSet resultSet = executeQuerySafely(statement, "SELECT * FROM INFORMATION_SCHEMA.CONSTRAINTS" +
                    " WHERE table_name = 'account' AND constraint_type = 'PRIMARY_KEY';");
            boolean resultIsNotEmpty = getNextSafely(resultSet);

            assertTrue(resultIsNotEmpty);
        });
    }

    @Test
    public void testPrimaryKeyHasCorrectName() throws SQLException {
        consumeStatement(statement -> {
            ResultSet resultSet = executeQuerySafely(statement, "SELECT * FROM INFORMATION_SCHEMA.CONSTRAINTS" +
                    " WHERE table_name = 'account' AND constraint_type = 'PRIMARY_KEY';");
            getNextSafely(resultSet);
            String pkConstraintName = getStringSafely(resultSet, "constraint_name");

            assertEquals("account_pk", pkConstraintName);
        });
    }

    @Test
    public void testPrimaryKeyBasedOnIdField() throws SQLException {
        consumeStatement(statement -> {
            ResultSet resultSet = executeQuerySafely(statement, "SELECT * FROM INFORMATION_SCHEMA.CONSTRAINTS" +
                    " WHERE table_name = 'account' AND constraint_type = 'PRIMARY_KEY';");
            getNextSafely(resultSet);
            String pkColumn = getStringSafely(resultSet, "column_list");

            assertEquals("id", pkColumn);
        });
    }

    @Test
    public void testTableHasCorrectAlternativeKey() throws SQLException {
        consumeStatement(statement -> {
            ResultSet resultSet = executeQuerySafely(statement, "SELECT * FROM INFORMATION_SCHEMA.CONSTRAINTS" +
                    " WHERE table_name = 'account' AND constraint_type = 'UNIQUE';");
            getNextSafely(resultSet);
            String uniqueConstraintName = getStringSafely(resultSet, "constraint_name");
            String uniqueConstraintColumn = getStringSafely(resultSet, "column_list");

            assertEquals("account_email_uq", uniqueConstraintName);
            assertEquals("email", uniqueConstraintColumn);
        });
    }

    @Test
    public void testTableHasAllRequiredColumns() throws SQLException {
        consumeStatement(statement -> {
            ResultSet resultSet = executeQuerySafely(statement, "SELECT * FROM INFORMATION_SCHEMA.COLUMNS" +
                    " WHERE table_name = 'account';");
            List<String> columns = fetchValues(resultSet, "column_name");

            assertEquals(8, columns.size());
            assertTrue(columns.containsAll(List.of("id", "first_name", "last_name", "email", "gender", "balance", "birthday", "creation_time")));
        });
    }


    @Test
    public void testRequiredColumnsHaveHaveNotNullConstraint() throws SQLException {
        consumeStatement(statement -> {
            ResultSet resultSet = executeQuerySafely(statement, "SELECT * FROM INFORMATION_SCHEMA.COLUMNS" +
                    " WHERE table_name = 'account' AND nullable = false;");
            List<String> notNullColumns = fetchValues(resultSet, "column_name");

            assertEquals(7, notNullColumns.size());
            assertTrue(notNullColumns.containsAll(List.of("id", "first_name", "last_name", "email", "gender", "birthday", "creation_time")));
        });
    }

    @Test
    public void testIdHasTypeBiInteger() throws SQLException {
        consumeStatement(statement -> {
            ResultSet resultSet = executeQuerySafely(statement, "SELECT * FROM INFORMATION_SCHEMA.COLUMNS" +
                    " WHERE table_name = 'account' AND column_name = 'id';");
            getNextSafely(resultSet);
            String idTypeName = getStringSafely(resultSet, "type_name");

            assertEquals("BIGINT", idTypeName);
        });
    }

    @Test
    public void testCreationTimeHasTypeTimestamp() throws SQLException {
        consumeStatement(statement -> {
            ResultSet resultSet = executeQuerySafely(statement, "SELECT * FROM INFORMATION_SCHEMA.COLUMNS" +
                    " WHERE table_name = 'account' AND column_name = 'creation_time';");
            getNextSafely(resultSet);
            String creationTimeColumnType = getStringSafely(resultSet, "type_name");

            assertEquals("TIMESTAMP", creationTimeColumnType);
        });
    }

    @Test
    public void testCreationTimeHasDefaultValue() throws SQLException {
        consumeStatement(statement -> {
            ResultSet resultSet = executeQuerySafely(statement, "SELECT * FROM INFORMATION_SCHEMA.COLUMNS" +
                    " WHERE table_name = 'account' AND column_name = 'creation_time';");
            getNextSafely(resultSet);
            String creationTimeColumnDefault = getStringSafely(resultSet, "column_default");

            assertEquals("NOW()", creationTimeColumnDefault);
        });
    }

    @Test
    public void testEmailColumnHasCorrectSize() throws SQLException {
        consumeStatement(statement -> {
            ResultSet resultSet = executeQuerySafely(statement, "SELECT * FROM INFORMATION_SCHEMA.COLUMNS" +
                    " WHERE table_name = 'account' AND column_name = 'email';");
            getNextSafely(resultSet);
            String emailColumnType = getStringSafely(resultSet, "type_name");
            int emailColumnMaxLength = getIntSafely(resultSet, "character_maximum_length");

            assertEquals("VARCHAR", emailColumnType);
            assertEquals(255, emailColumnMaxLength);
        });
    }

    @Test
    public void testBirthdayColumnHasCorrectType() throws SQLException {
        consumeStatement(statement -> {
            ResultSet resultSet = executeQuerySafely(statement, "SELECT * FROM INFORMATION_SCHEMA.COLUMNS" +
                    " WHERE table_name = 'account' AND column_name = 'birthday';");
            getNextSafely(resultSet);
            String birthdayColumnType = getStringSafely(resultSet, "type_name");

            assertEquals("DATE", birthdayColumnType);
        });
    }

    @Test
    public void testBalanceColumnHasCorrectType() throws SQLException {
        consumeStatement(statement -> {
            ResultSet resultSet = executeQuerySafely(statement, "SELECT * FROM INFORMATION_SCHEMA.COLUMNS" +
                    " WHERE table_name = 'account' AND column_name = 'balance';");
            getNextSafely(resultSet);
            String balanceColumnType = getStringSafely(resultSet, "type_name");
            int balanceColumnPrecision = getIntSafely(resultSet, "numeric_precision");
            int balanceColumnScale = getIntSafely(resultSet, "numeric_scale");

            assertEquals("DECIMAL", balanceColumnType);
            assertEquals(19, balanceColumnPrecision);
            assertEquals(4, balanceColumnScale);
        });
    }

    @Test
    public void testBalanceIsNotMandatory() throws SQLException {
        consumeStatement(statement -> {
            ResultSet resultSet = executeQuerySafely(statement, "SELECT * FROM INFORMATION_SCHEMA.COLUMNS" +
                    " WHERE table_name = 'account' AND column_name = 'balance';");
            getNextSafely(resultSet);
            boolean balanceColumnIsNullable = getBooleanSafely(resultSet, "nullable");

            assertTrue(balanceColumnIsNullable);
        });
    }

    @Test
    public void testStringColumnsHaveCorrectTypeAndLength() throws SQLException {
        consumeStatement(statement -> {
            ResultSet resultSet = executeQuerySafely(statement, "SELECT column_name FROM INFORMATION_SCHEMA.COLUMNS" +
                    " WHERE table_name = 'account' AND type_name = 'VARCHAR' AND character_maximum_length = 255;");
            List<String> stringColumns = fetchValues(resultSet, "column_name");

            assertEquals(4, stringColumns.size());
            assertTrue(stringColumns.containsAll(List.of("first_name", "last_name", "email", "gender")));
        });
    }
}
