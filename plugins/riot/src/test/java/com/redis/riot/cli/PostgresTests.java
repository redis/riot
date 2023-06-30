package com.redis.riot.cli;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.redis.lettucemod.api.sync.RedisModulesCommands;
import com.redis.spring.batch.reader.GeneratorItemReader;

@SuppressWarnings("unchecked")
class PostgresTests extends AbstractDatabaseTests {

	private static final DockerImageName POSTGRES_IMAGE = DockerImageName.parse(PostgreSQLContainer.IMAGE)
			.withTag(PostgreSQLContainer.DEFAULT_TAG);

	private static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>(POSTGRES_IMAGE);

	@Override
	protected JdbcDatabaseContainer<?> getJdbcDatabaseContainer() {
		return POSTGRES;
	}

	@BeforeAll
	void setupPostgres() throws SQLException, IOException {
		executeScript("db/northwind.sql");
	}

	@BeforeEach
	void clearTables() throws SQLException {
		try (Statement statement = databaseConnection.createStatement()) {
			statement.execute("DROP TABLE IF EXISTS mytable");
		}
	}

	@Test
	void export() throws Exception {
		String filename = "db-export-postgresql";
		try (Statement statement = databaseConnection.createStatement()) {
			statement.execute("CREATE TABLE mytable (id smallint NOT NULL, field1 bpchar, field2 bpchar)");
			statement.execute("ALTER TABLE ONLY mytable ADD CONSTRAINT pk_mytable PRIMARY KEY (id)");
			GeneratorItemReader generator = generator();
			generator.setTypes(GeneratorItemReader.Type.HASH);
			generate(filename, DEFAULT_BATCH_SIZE, generator);
			execute(filename);
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
			Assertions.assertEquals(connection.sync().dbsize(), count);
		}
	}

	@Test
	void nullValueExport() throws Exception {
		try (Statement statement = databaseConnection.createStatement()) {
			statement.execute("CREATE TABLE mytable (id smallint NOT NULL, field1 bpchar, field2 bpchar)");
			statement.execute("ALTER TABLE ONLY mytable ADD CONSTRAINT pk_mytable PRIMARY KEY (id)");
			RedisModulesCommands<String, String> sync = connection.sync();
			Map<String, String> hash1 = new HashMap<>();
			hash1.put("field1", "value1");
			hash1.put("field2", "value2");
			sync.hmset("gen:1", hash1);
			Map<String, String> hash2 = new HashMap<>();
			hash2.put("field2", "value2");
			sync.hmset("gen:2", hash2);
			execute("db-export-postgresql");
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
			Assertions.assertEquals(sync.dbsize().longValue(), index);
		}
	}

	@Test
	void hashImport() throws Exception {
		execute("db-import-postgresql");
		try (Statement statement = databaseConnection.createStatement()) {
			statement.execute("SELECT COUNT(*) AS count FROM orders");
			RedisModulesCommands<String, String> sync = connection.sync();
			List<String> keys = sync.keys("order:*");
			ResultSet resultSet = statement.getResultSet();
			resultSet.next();
			Assertions.assertEquals(resultSet.getLong("count"), keys.size());
			Map<String, String> order = sync.hgetall("order:10248");
			Assertions.assertEquals("10248", order.get("order_id"));
			Assertions.assertEquals("VINET", order.get("customer_id"));
		}
	}

	@Test
	void noopImport() throws Exception {
		execute("db-import-postgresql-noop");
		Assertions.assertEquals(0, connection.sync().dbsize());
	}

	@Test
	void multiThreadedImport() throws Exception {
		execute("db-import-postgresql-multithreaded");
		try (Statement statement = databaseConnection.createStatement()) {
			statement.execute("SELECT COUNT(*) AS count FROM orders");
			RedisModulesCommands<String, String> sync = connection.sync();
			List<String> keys = sync.keys("order:*");
			ResultSet resultSet = statement.getResultSet();
			resultSet.next();
			Awaitility.await().until(() -> resultSet.getLong("count") == keys.size());
			Map<String, String> order = sync.hgetall("order:10248");
			Assertions.assertEquals("10248", order.get("order_id"));
			Assertions.assertEquals("VINET", order.get("customer_id"));
		}
	}

	@Test
	void setImport() throws Exception {
		execute("db-import-postgresql-set");
		try (Statement statement = databaseConnection.createStatement()) {
			statement.execute("SELECT * FROM orders");
			ResultSet resultSet = statement.getResultSet();
			RedisModulesCommands<String, String> sync = connection.sync();
			long count = 0;
			while (resultSet.next()) {
				int orderId = resultSet.getInt("order_id");
				String order = sync.get("order:" + orderId);
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
			List<String> keys = sync.keys("order:*");
			Assertions.assertEquals(count, keys.size());
		}
	}

}
