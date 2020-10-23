package com.redislabs.riot.db;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import javax.sql.DataSource;

import org.junit.Assert;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.batch.item.redis.support.DataType;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.redislabs.riot.test.DataPopulator;

@Testcontainers
@SuppressWarnings("rawtypes")
public class TestPostgreSQL extends DbTest {

	@Container
	private static final PostgreSQLContainer postgreSQL = new PostgreSQLContainer(
			DockerImageName.parse(PostgreSQLContainer.IMAGE).withTag(PostgreSQLContainer.DEFAULT_TAG));

	@Test
	public void testExport() throws Exception {
		DataSource dataSource = dataSource(postgreSQL);
		Connection connection = dataSource.getConnection();
		Statement statement = connection.createStatement();
		statement.execute("CREATE TABLE mytable (id smallint NOT NULL, field1 bpchar, field2 bpchar)");
		statement.execute("ALTER TABLE ONLY mytable ADD CONSTRAINT pk_mytable PRIMARY KEY (id)");
		DataPopulator.builder().connection(connection()).dataTypes(Collections.singletonList(DataType.HASH)).build()
				.run();
		executeFile("/postgresql/export.txt");
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
		ScriptRunner scriptRunner = ScriptRunner.builder().connection(connection).autoCommit(false).stopOnError(true)
				.build();
		InputStream inputStream = getClass().getClassLoader().getResourceAsStream("postgresql/northwind.sql");
		scriptRunner.runScript(new InputStreamReader(inputStream));
		executeFile("/postgresql/import.txt");
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

	@Test
	public void testImportToJsonStrings() throws Exception {
		DataSource dataSource = dataSource(postgreSQL);
		Connection connection = dataSource.getConnection();
		ScriptRunner scriptRunner = ScriptRunner.builder().connection(connection).autoCommit(false).stopOnError(true)
				.build();
		InputStream inputStream = getClass().getClassLoader().getResourceAsStream("postgresql/northwind.sql");
		scriptRunner.runScript(new InputStreamReader(inputStream));
		executeFile("/postgresql/import-to-json-strings.txt");
		Statement statement = connection.createStatement();
		statement.execute("SELECT * FROM orders");
		ResultSet resultSet = statement.getResultSet();
		long count = 0;
		while (resultSet.next()) {
			int orderId = resultSet.getInt("order_id");
			String order = commands().get("order:" + orderId);
			ObjectMapper mapper = new ObjectMapper();
			ObjectReader reader = mapper.readerFor(Map.class);
			Map<String, Object> orderMap = reader.readValue(order);
			Assert.assertEquals(orderId, orderMap.get("order_id"));
			Assert.assertEquals(resultSet.getString("customer_id"), orderMap.get("customer_id"));
			Assert.assertEquals(resultSet.getInt("employee_id"), orderMap.get("employee_id"));
			Assert.assertEquals(resultSet.getDate("order_date"), java.sql.Date
					.valueOf(LocalDate.from(DateTimeFormatter.ISO_DATE.parse((String) orderMap.get("order_date")))));
			Assert.assertEquals(resultSet.getFloat("freight"), ((Double) orderMap.get("freight")).floatValue(), 0);
			count++;
		}
		List<String> keys = commands().keys("order:*");
		Assertions.assertEquals(count, keys.size());
		connection.close();
	}

	@Override
	protected String process(String command) {
		return super.process(command).replace("jdbc:postgresql://host:port/database", postgreSQL.getJdbcUrl())
				.replace("appuser", postgreSQL.getUsername()).replace("passwd", postgreSQL.getPassword());
	}
}
