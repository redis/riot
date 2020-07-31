package com.redislabs.riot.db;

import com.redislabs.riot.test.DataPopulator;
import org.junit.Assert;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.batch.item.redis.support.DataType;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import javax.sql.DataSource;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Collections;
import java.util.List;
import java.util.Map;

@Testcontainers
@SuppressWarnings("rawtypes")
public class TestPostgreSQL extends DbTest {

    @Container
    private static final PostgreSQLContainer postgreSQL = new PostgreSQLContainer();

    @Test
    public void testExport() throws Exception {
        DataSource dataSource = dataSource(postgreSQL);
        Connection connection = dataSource.getConnection();
        Statement statement = connection.createStatement();
        statement.execute("CREATE TABLE mytable (id smallint NOT NULL, field1 bpchar, field2 bpchar)");
        statement.execute("ALTER TABLE ONLY mytable ADD CONSTRAINT pk_mytable PRIMARY KEY (id)");
        DataPopulator.builder().connection(connection()).dataTypes(Collections.singletonList(DataType.HASH)).build().run();
        runFile("/postgresql/export.txt");
        statement.execute("SELECT COUNT(*) AS count FROM mytable");
        ResultSet countResultSet = statement.getResultSet();
        countResultSet.next();
        statement.execute("SELECT * from mytable ORDER BY id ASC");
        ResultSet resultSet = statement.getResultSet();
        int index = 0;
        while (resultSet.next()) {
            Assertions.assertEquals(index, resultSet.getInt("id"));
            index++;
        }
        Assertions.assertEquals(commands().dbsize().longValue(), index);
    }

    @Test
    public void testImport() throws Exception {
        DataSource dataSource = dataSource(postgreSQL);
        Connection connection = dataSource.getConnection();
        ScriptRunner scriptRunner = ScriptRunner.builder().connection(connection).autoCommit(false).stopOnError(true).build();
        InputStream inputStream = getClass().getClassLoader().getResourceAsStream("postgresql/northwind.sql");
        scriptRunner.runScript(new InputStreamReader(inputStream));
        runFile("/postgresql/import.txt");
        Statement statement = connection.createStatement();
        statement.execute("SELECT COUNT(*) AS count FROM orders");
        List<String> keys = commands().keys("order:*");
        ResultSet resultSet = statement.getResultSet();
        resultSet.next();
        Assertions.assertEquals(resultSet.getLong("count"), keys.size());
        Map<String, String> order = commands().hgetall("order:10248");
        Assert.assertEquals("10248", order.get("order_id"));
        Assert.assertEquals("VINET", order.get("customer_id"));
        connection.close();
    }

    @Override
    protected String filter(String command) {
        return super.filter(command).replace("jdbc:postgresql://host:port/database", postgreSQL.getJdbcUrl()).replace("appuser", postgreSQL.getUsername()).replace("passwd", postgreSQL.getPassword());
    }
}
