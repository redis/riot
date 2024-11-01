package com.redis.riot;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.redis.spring.batch.item.redis.gen.GeneratorItemReader;
import com.redis.spring.batch.item.redis.gen.ItemType;

@EnabledOnOs(value = OS.LINUX)
class PostgresTests extends DbTests {

	private static final DockerImageName postgresImage = DockerImageName.parse(PostgreSQLContainer.IMAGE)
			.withTag(PostgreSQLContainer.DEFAULT_TAG);
	private static final PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>(postgresImage);

	@Override
	protected JdbcDatabaseContainer<?> getJdbcDatabaseContainer() {
		return postgres;
	}

	@BeforeAll
	void setupPostgres() throws SQLException, IOException {
		executeScript("db/northwind.sql");
	}

	@BeforeEach
	void clearTables() throws SQLException {
		try (Statement statement = dbConnection.createStatement()) {
			statement.execute("DROP TABLE IF EXISTS mytable");
		}
	}

	@Test
	void export(TestInfo info) throws Exception {
		String filename = "db-export-postgresql";
		try (Statement statement = dbConnection.createStatement()) {
			statement.execute("CREATE TABLE mytable (id smallint NOT NULL, field1 bpchar, field2 bpchar)");
			statement.execute("ALTER TABLE ONLY mytable ADD CONSTRAINT pk_mytable PRIMARY KEY (id)");
			GeneratorItemReader generator = generator(73, ItemType.HASH);
			generate(info, generator);
			execute(info, filename, r -> executeDatabaseExport(r, info));
			statement.execute("SELECT COUNT(*) AS count FROM mytable");
			ResultSet countResultSet = statement.getResultSet();
			countResultSet.next();
			statement.execute("SELECT * from mytable");
			ResultSet resultSet = statement.getResultSet();
			long count = 0;
			while (resultSet.next()) {
				Assertions.assertTrue(resultSet.getInt("id") >= 0);
				Assertions.assertNotNull(resultSet.getString("field1"));
				Assertions.assertNotNull(resultSet.getString("field2"));
				count++;
			}
			Assertions.assertEquals(redisCommands.dbsize(), count);
		}
	}

	@Test
	void nullValueExport(TestInfo info) throws Exception {
		try (Statement statement = dbConnection.createStatement()) {
			statement.execute("CREATE TABLE mytable (id smallint NOT NULL, field1 bpchar, field2 bpchar)");
			statement.execute("ALTER TABLE ONLY mytable ADD CONSTRAINT pk_mytable PRIMARY KEY (id)");
			Map<String, String> hash1 = new HashMap<>();
			hash1.put("field1", "value1");
			hash1.put("field2", "value2");
			redisCommands.hmset("gen:1", hash1);
			Map<String, String> hash2 = new HashMap<>();
			hash2.put("field2", "value2");
			redisCommands.hmset("gen:2", hash2);
			execute(info, "db-export-postgresql", r -> executeDatabaseExport(r, info));
			statement.execute("SELECT COUNT(*) AS count FROM mytable");
			ResultSet countResultSet = statement.getResultSet();
			countResultSet.next();
			Assertions.assertEquals(2, countResultSet.getInt(1));
			statement.execute("SELECT * from mytable ORDER BY id ASC");
			ResultSet resultSet = statement.getResultSet();
			int index = 0;
			while (resultSet.next()) {
				Assertions.assertEquals(index + 1, resultSet.getInt("id"));
				index++;
			}
			Assertions.assertEquals(redisCommands.dbsize().longValue(), index);
		}
	}

	@Test
	void hashImport(TestInfo info) throws Exception {
		execute(info, "db-import-postgresql", this::executeDatabaseImport);
		try (Statement statement = dbConnection.createStatement()) {
			statement.execute("SELECT COUNT(*) AS count FROM orders");
			ResultSet resultSet = statement.getResultSet();
			resultSet.next();
			Assertions.assertEquals(resultSet.getLong("count"), keyCount("order:*"));
			Map<String, String> order = redisCommands.hgetall("order:10248");
			Assertions.assertEquals("10248", order.get("order_id"));
			Assertions.assertEquals("VINET", order.get("customer_id"));
		}
	}

	@Test
	void multiThreadedImport(TestInfo info) throws Exception {
		execute(info, "db-import-postgresql-multithreaded", this::executeDatabaseImport);
		int count = keyCount("order:*");
		try (Statement statement = dbConnection.createStatement()) {
			try (ResultSet resultSet = statement.executeQuery("SELECT COUNT(*) AS count FROM orders")) {
				Assertions.assertTrue(resultSet.next());
				Assertions.assertEquals(resultSet.getLong("count"), count);
			}
			Map<String, String> order = redisCommands.hgetall("order:10248");
			Assertions.assertEquals("10248", order.get("order_id"));
			Assertions.assertEquals("VINET", order.get("customer_id"));
		}
	}

	@Test
	void setImport(TestInfo info) throws Exception {
		execute(info, "db-import-postgresql-set", this::executeDatabaseImport);
		try (Statement statement = dbConnection.createStatement()) {
			statement.execute("SELECT * FROM orders");
			ResultSet resultSet = statement.getResultSet();
			long count = 0;
			while (resultSet.next()) {
				int orderId = resultSet.getInt("order_id");
				String order = redisCommands.get("order:" + orderId);
				ObjectMapper mapper = new ObjectMapper();
				ObjectReader reader = mapper.readerFor(Map.class);
				Map<String, Object> orderMap = reader.readValue(order);
				Assertions.assertEquals(orderId, orderMap.get("order_id"));
				Assertions.assertEquals(resultSet.getString("customer_id"), orderMap.get("customer_id"));
				Assertions.assertEquals(resultSet.getInt("employee_id"), orderMap.get("employee_id"));
				Assertions.assertEquals(resultSet.getDate("order_date"), java.sql.Date.valueOf(
						LocalDate.from(DateTimeFormatter.ISO_DATE.parse((String) orderMap.get("order_date")))));
				Assertions.assertEquals(resultSet.getFloat("freight"), ((Double) orderMap.get("freight")).floatValue(),
						0);
				count++;
			}
			Assertions.assertEquals(count, keyCount("order:*"));
		}
	}

}
