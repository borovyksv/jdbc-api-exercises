package com.bobocode;

import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import static com.bobocode.util.JdbcUtil.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;


public class WallStreetDbInitializerTest extends DbInitializerTestBase {

    @BeforeClass
    public static void init() throws SQLException {
        initDataSource(new WallStreetDbInitializer());
    }

    // table broker tests

    @Test
    public void testTablesHaveCorrectNames() throws SQLException {
        consumeStatement(statement -> {
            executeSafely(statement, "SHOW TABLES");
            ResultSet resultSet = executeQuerySafely(statement,"SHOW TABLES");
            List<String> tableNames = fetchValues(resultSet, "table_name");

            assertThat(tableNames, containsInAnyOrder("broker", "sales_group", "broker_sales_group"));
        });
    }

    @Test
    public void testBrokerTablesHasPrimaryKey() throws SQLException {
        consumeStatement(statement -> {
            ResultSet resultSet = executeQuerySafely(statement, "SELECT * FROM INFORMATION_SCHEMA.CONSTRAINTS" +
                    " WHERE table_name = 'broker' AND constraint_type = 'PRIMARY_KEY';");
            boolean resultIsNotEmpty = getNextSafely(resultSet);

            assertThat(resultIsNotEmpty, is(true));
        });
    }

    @Test
    public void testBrokerTablePrimaryKeyHasCorrectName() throws SQLException {
        consumeStatement(statement -> {
            ResultSet resultSet = executeQuerySafely(statement,"SELECT * FROM INFORMATION_SCHEMA.CONSTRAINTS" +
                    " WHERE table_name = 'broker' AND constraint_type = 'PRIMARY_KEY';");
            getNextSafely(resultSet);
            String pkConstraintName = getStringSafely(resultSet, "constraint_name");

            assertThat(pkConstraintName, equalTo("PK_broker"));
        });
    }

    @Test
    public void testBrokerTablePrimaryKeyBasedOnIdField() throws SQLException {
        consumeStatement(statement -> {
            ResultSet resultSet = executeQuerySafely(statement,"SELECT * FROM INFORMATION_SCHEMA.CONSTRAINTS" +
                    " WHERE table_name = 'broker' AND constraint_type = 'PRIMARY_KEY';");
            getNextSafely(resultSet);
            String pkColumn = getStringSafely(resultSet, "column_list");

            assertThat("id", equalTo(pkColumn));
        });
    }
    
    @Test
    public void testBrokerTableHasCorrectUniqueConstraint() throws SQLException {
        consumeStatement(statement -> {
            ResultSet resultSet = executeQuerySafely(statement,"SELECT * FROM INFORMATION_SCHEMA.CONSTRAINTS" +
                    " WHERE table_name = 'broker' AND constraint_type = 'UNIQUE';");
            getNextSafely(resultSet);
            String uniqueConstraintName = getStringSafely(resultSet, "constraint_name");
            String uniqueConstraintColumn = getStringSafely(resultSet, "column_list");

            assertThat(uniqueConstraintName, equalTo("UQ_broker_username"));
            assertThat(uniqueConstraintColumn, equalTo("username"));
        });
    }

    @Test
    public void testBrokerTableHasAllRequiredColumns() throws SQLException {
        consumeStatement(statement -> {
            ResultSet resultSet = executeQuerySafely(statement,"SELECT * FROM INFORMATION_SCHEMA.COLUMNS" +
                    " WHERE table_name = 'broker';");
            List<String> columns = fetchValues(resultSet, "column_name");

            assertThat(columns.size(), equalTo(4));
            assertThat(columns, containsInAnyOrder("id", "username", "first_name", "last_name"));
        });
    }

    @Test
    public void testBrokerTableRequiredColumnsHaveHaveNotNullConstraint() throws SQLException {
        consumeStatement(statement -> {
            ResultSet resultSet = executeQuerySafely(statement,"SELECT * FROM INFORMATION_SCHEMA.COLUMNS" +
                    " WHERE table_name = 'broker' AND nullable = false;");
            List<String> notNullColumns = fetchValues(resultSet, "column_name");

            assertThat(notNullColumns.size(), is(4));
            assertThat(notNullColumns, containsInAnyOrder("id", "username", "first_name", "last_name"));
        });
    }

    @Test
    public void testBrokerIdTypeIsBigint() throws SQLException {
        consumeStatement(statement -> {
            ResultSet resultSet = executeQuerySafely(statement,"SELECT * FROM INFORMATION_SCHEMA.COLUMNS" +
                    " WHERE table_name = 'broker' AND column_name = 'id';");
            getNextSafely(resultSet);
            String idTypeName = getStringSafely(resultSet, "type_name");

            assertThat(idTypeName, is("BIGINT"));
        });
    }

    @Test
    public void testBrokerTableStringColumnsHaveCorrectTypeAndLength() throws SQLException {
        consumeStatement(statement -> {
            ResultSet resultSet = executeQuerySafely(statement,"SELECT column_name FROM INFORMATION_SCHEMA.COLUMNS" +
                    " WHERE table_name = 'broker' AND type_name = 'VARCHAR' AND character_maximum_length = 255;");
            List<String> stringColumns = fetchValues(resultSet, "column_name");

            assertThat(stringColumns.size(), is(3));
            assertThat(stringColumns, containsInAnyOrder("username", "first_name", "last_name"));
        });
    }

    // table sale_group test

    @Test
    public void testSaleGroupTablesHasPrimaryKey() throws SQLException {
        consumeStatement(statement -> {
            ResultSet resultSet = executeQuerySafely(statement,"SELECT * FROM INFORMATION_SCHEMA.CONSTRAINTS" +
                    " WHERE table_name = 'sales_group' AND constraint_type = 'PRIMARY_KEY';");
            boolean resultIsNotEmpty = getNextSafely(resultSet);

            assertThat(resultIsNotEmpty, is(true));
        });
    }

    @Test
    public void testSaleGroupTablePrimaryKeyHasCorrectName() throws SQLException {
        consumeStatement(statement -> {
            ResultSet resultSet = executeQuerySafely(statement,"SELECT * FROM INFORMATION_SCHEMA.CONSTRAINTS" +
                    " WHERE table_name = 'sales_group' AND constraint_type = 'PRIMARY_KEY';");
            getNextSafely(resultSet);
            String pkConstraintName = getStringSafely(resultSet, "constraint_name");

            assertThat(pkConstraintName, equalTo("PK_sales_group"));
        });
    }

    @Test
    public void testSaleGroupTablePrimaryKeyBasedOnIdField() throws SQLException {
        consumeStatement(statement -> {
            ResultSet resultSet = executeQuerySafely(statement,"SELECT * FROM INFORMATION_SCHEMA.CONSTRAINTS" +
                    " WHERE table_name = 'sales_group' AND constraint_type = 'PRIMARY_KEY';");
            getNextSafely(resultSet);
            String pkColumn = getStringSafely(resultSet, "column_list");

            assertThat("id", equalTo(pkColumn));
        });
    }

    @Test
    public void testSaleGroupTableHasCorrectUniqueConstraint() throws SQLException {
        consumeStatement(statement -> {
            ResultSet resultSet = executeQuerySafely(statement,"SELECT * FROM INFORMATION_SCHEMA.CONSTRAINTS" +
                    " WHERE table_name = 'sales_group' AND constraint_type = 'UNIQUE';");
            getNextSafely(resultSet);
            String uniqueConstraintName = getStringSafely(resultSet, "constraint_name");
            String uniqueConstraintColumn = getStringSafely(resultSet, "column_list");

            assertThat(uniqueConstraintName, equalTo("UQ_sales_group_name"));
            assertThat(uniqueConstraintColumn, equalTo("name"));
        });
    }

    @Test
    public void testSaleGroupTableHasAllRequiredColumns() throws SQLException {
        consumeStatement(statement -> {
            ResultSet resultSet = executeQuerySafely(statement,"SELECT * FROM INFORMATION_SCHEMA.COLUMNS" +
                    " WHERE table_name = 'sales_group';");
            List<String> columns = fetchValues(resultSet, "column_name");

            assertThat(columns.size(), equalTo(4));
            assertThat(columns, containsInAnyOrder("id", "name", "transaction_type", "max_transaction_amount"));
        });
    }

    @Test
    public void testSaleGroupTableRequiredColumnsHaveHaveNotNullConstraint() throws SQLException {
        consumeStatement(statement -> {
            ResultSet resultSet = executeQuerySafely(statement,"SELECT * FROM INFORMATION_SCHEMA.COLUMNS" +
                    " WHERE table_name = 'sales_group' AND nullable = false;");
            List<String> notNullColumns = fetchValues(resultSet, "column_name");

            assertThat(notNullColumns.size(), is(4));
            assertThat(notNullColumns, containsInAnyOrder("id", "name", "transaction_type", "max_transaction_amount"));
        });
    }

    @Test
    public void testSaleGroupIdTypeIsBigint() throws SQLException {
        consumeStatement(statement -> {
            ResultSet resultSet = executeQuerySafely(statement,"SELECT * FROM INFORMATION_SCHEMA.COLUMNS" +
                    " WHERE table_name = 'sales_group' AND column_name = 'id';");
            getNextSafely(resultSet);
            String idTypeName = getStringSafely(resultSet, "type_name");

            assertThat(idTypeName, is("BIGINT"));
        });
    }

    @Test
    public void testSaleGroupTableStringColumnsHaveCorrectTypeAndLength() throws SQLException {
        consumeStatement(statement -> {
            ResultSet resultSet = executeQuerySafely(statement,"SELECT column_name FROM INFORMATION_SCHEMA.COLUMNS" +
                    " WHERE table_name = 'sales_group' AND type_name = 'VARCHAR' AND character_maximum_length = 255;");
            List<String> stringColumns = fetchValues(resultSet, "column_name");

            assertThat(stringColumns.size(), is(2));
            assertThat(stringColumns, containsInAnyOrder("name", "transaction_type"));
        });
    }

    // table broker_sales_group tests

    @Test
    public void testBrokerSaleGroupTablesHasForeignKeyToBroker() throws SQLException {
        consumeStatement(statement -> {
            ResultSet resultSet = executeQuerySafely(statement,"SELECT * FROM INFORMATION_SCHEMA.CONSTRAINTS" +
                    " WHERE table_name = 'broker_sales_group' AND constraint_type = 'REFERENTIAL' AND column_list = 'broker_id';");

            boolean resultIsNotEmpty = getNextSafely(resultSet);

            assertThat(resultIsNotEmpty, is(true));
        });
    }

    @Test
    public void testBrokerSaleGroupTableForeignKeyToBrokerHasCorrectName() throws SQLException {
        consumeStatement(statement -> {
            ResultSet resultSet = executeQuerySafely(statement,"SELECT * FROM INFORMATION_SCHEMA.CONSTRAINTS" +
                    " WHERE table_name = 'broker_sales_group' AND constraint_type = 'REFERENTIAL' AND column_list = 'broker_id';");
            getNextSafely(resultSet);
            String fkConstraintName = getStringSafely(resultSet, "constraint_name");

            assertThat(fkConstraintName, equalTo("FK_broker_sales_group_broker"));
        });
    }

    @Test
    public void testBrokerSaleGroupTablesHasForeignKeyToSalesGroup() throws SQLException {
        consumeStatement(statement -> {
            ResultSet resultSet = executeQuerySafely(statement,"SELECT * FROM INFORMATION_SCHEMA.CONSTRAINTS" +
                    " WHERE table_name = 'broker_sales_group' AND constraint_type = 'REFERENTIAL' AND column_list = 'sales_group_id';");
            boolean resultIsNotEmpty = getNextSafely(resultSet);

            assertThat(resultIsNotEmpty, is(true));
        });
    }

    @Test
    public void testBrokerSaleGroupTableForeignKeyToSalesGroupHasCorrectName() throws SQLException {
        consumeStatement(statement -> {
            ResultSet resultSet = executeQuerySafely(statement,"SELECT * FROM INFORMATION_SCHEMA.CONSTRAINTS" +
                    " WHERE table_name = 'broker_sales_group' AND constraint_type = 'REFERENTIAL' AND column_list = 'sales_group_id';");
            getNextSafely(resultSet);
            String fkConstraintName = getStringSafely(resultSet, "constraint_name");

            assertThat(fkConstraintName, equalTo("FK_broker_sales_group_sales_group"));
        });
    }

    @Test
    public void testBrokerSaleGroupTableHasNotNullConstraint() throws SQLException {
        consumeStatement(statement -> {
            ResultSet resultSet = executeQuerySafely(statement,"SELECT * FROM INFORMATION_SCHEMA.COLUMNS" +
                    " WHERE table_name = 'broker_sales_group' AND nullable = false;");
            List<String> notNullColumns = fetchValues(resultSet, "column_name");

            assertThat(notNullColumns.size(), is(2));
            assertThat(notNullColumns, containsInAnyOrder("broker_id", "sales_group_id"));
        });
    }

    @Test
    public void testBrokerSaleGroupForeignKeysTypeAreBigint() throws SQLException {
        consumeStatement(statement -> {
            ResultSet resultSet = executeQuerySafely(statement,"SELECT * FROM INFORMATION_SCHEMA.COLUMNS" +
                    " WHERE table_name = 'broker_sales_group';");
            List<String> columnTypes = fetchValues(resultSet, "type_name");

            assertThat(columnTypes, contains("BIGINT", "BIGINT"));
        });
    }

    @Test
    public void testBrokerSaleGroupTableHasCompositePrimaryKey() throws SQLException {
        consumeStatement(statement -> {
            ResultSet resultSet = executeQuerySafely(statement,"SELECT * FROM INFORMATION_SCHEMA.CONSTRAINTS" +
                    " WHERE table_name = 'broker_sales_group' AND constraint_type = 'PRIMARY_KEY';");
            getNextSafely(resultSet);
            String uniqueConstraintName = getStringSafely(resultSet, "constraint_name");
            String uniqueConstraintColumn = getStringSafely(resultSet, "column_list");

            assertThat(uniqueConstraintName, equalTo("PK_broker_sales_group"));
            assertThat(uniqueConstraintColumn, equalTo("broker_id,sales_group_id"));
        });
    }
}
