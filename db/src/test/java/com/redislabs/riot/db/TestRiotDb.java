package com.redislabs.riot.db;

import com.redislabs.riot.test.BaseTest;
import com.redislabs.riot.test.DataPopulator;
import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.batch.item.redis.support.DataType;
import org.springframework.boot.autoconfigure.jdbc.DataSourceProperties;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Collections;
import java.util.List;

public class TestRiotDb extends BaseTest {

    private final static int COUNT = 1234;

    @Override
    protected int execute(String[] args) {
        return new App().execute(args);
    }

    @Override
    protected String applicationName() {
        return "riot-db";
    }

    private void populateBeersTable() throws Exception {
        Connection connection = dataSource().getConnection();
        connection.createStatement().execute("DROP TABLE IF EXISTS mytable");
        connection.createStatement().execute("CREATE TABLE IF NOT EXISTS mytable (id INT NOT NULL, field1 VARCHAR(500), field2 VARCHAR(500), PRIMARY KEY (id))");
        DataPopulator.builder().connection(super.connection).dataTypes(Collections.singletonList(DataType.HASH)).start(0).end(1234).build().run();
        runFile("/export-db.txt");
        commands().flushall();
    }

    private DataSource dataSource() {
        DataSourceProperties properties = new DataSourceProperties();
        properties.setUrl("jdbc:hsqldb:mem:mymemdb");
        properties.setDriverClassName("org.hsqldb.jdbc.JDBCDriver");
        return properties.initializeDataSourceBuilder().type(HikariDataSource.class).build();
    }

    @Test
    public void testExportToDatabase() throws Exception {
        populateBeersTable();
        Statement statement = dataSource().getConnection().createStatement();
        statement.execute("SELECT * from mytable ORDER BY id ASC");
        ResultSet resultSet = statement.getResultSet();
        int index = 0;
        while (resultSet.next()) {
            Assertions.assertTrue(resultSet.getInt("id") == index);
            index++;
        }
        Assertions.assertEquals(COUNT, index);
    }

    @Test
    public void testImportDatabase() throws Exception {
        populateBeersTable();
        runFile("/import-db.txt");
        List<String> keys = commands().keys("dbtest:*");
        Assertions.assertEquals(COUNT, keys.size());
    }

}
