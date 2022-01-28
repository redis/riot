package com.redis.riot.db;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.redis.lettucemod.api.sync.RedisModulesCommands;
import com.redis.spring.batch.DataStructure.Type;
import com.redis.testcontainers.junit.RedisTestContext;
import com.redis.testcontainers.junit.RedisTestContextsSource;

@Testcontainers
@SuppressWarnings({ "rawtypes", "unchecked" })
public class PostgreSQLTests extends AbstractDatabaseTests {

	private static final DockerImageName POSTGRE_DOCKER_IMAGE_NAME = DockerImageName.parse(PostgreSQLContainer.IMAGE)
			.withTag(PostgreSQLContainer.DEFAULT_TAG);

	@Container
	private static final PostgreSQLContainer POSTGRESQL = new PostgreSQLContainer(POSTGRE_DOCKER_IMAGE_NAME);
	private static Connection connection;

	@BeforeAll
	public static void setupAll() throws SQLException, IOException {
		connection = dataSource(POSTGRESQL).getConnection();
		ScriptRunner scriptRunner = new ScriptRunner(connection);
		scriptRunner.setAutoCommit(false);
		scriptRunner.setStopOnError(true);
		String file = "northwind.sql";
		InputStream inputStream = PostgreSQLTests.class.getClassLoader().getResourceAsStream(file);
		if (inputStream == null) {
			throw new FileNotFoundException(file);
		}
		scriptRunner.runScript(new InputStreamReader(inputStream));
	}

	@AfterAll
	public static void teardownAll() throws SQLException {
		connection.close();
	}

	@AfterEach
	void clearTables() throws SQLException {
		try (Statement statement = connection.createStatement()) {
			statement.execute("DROP TABLE IF EXISTS mytable");
		}
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void testExport(RedisTestContext redis) throws Exception {
		try (Statement statement = connection.createStatement()) {
			statement.execute("CREATE TABLE mytable (id smallint NOT NULL, field1 bpchar, field2 bpchar)");
			statement.execute("ALTER TABLE ONLY mytable ADD CONSTRAINT pk_mytable PRIMARY KEY (id)");
			generator(redis, "postgre-export").type(Type.HASH).build().call();
			execute("export-postgresql", redis, r -> configureExportCommand(r, POSTGRESQL));
			statement.execute("SELECT COUNT(*) AS count FROM mytable");
			ResultSet countResultSet = statement.getResultSet();
			countResultSet.next();
			statement.execute("SELECT * from mytable ORDER BY id ASC");
			ResultSet resultSet = statement.getResultSet();
			long index = 1;
			while (resultSet.next()) {
				Assertions.assertEquals(index, resultSet.getInt("id"));
				index++;
			}
			Assertions.assertEquals(redis.sync().dbsize(), index - 1);
		}
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void testExportNullValues(RedisTestContext redis) throws Exception {
		try (Statement statement = connection.createStatement()) {
			statement.execute("CREATE TABLE mytable (id smallint NOT NULL, field1 bpchar, field2 bpchar)");
			statement.execute("ALTER TABLE ONLY mytable ADD CONSTRAINT pk_mytable PRIMARY KEY (id)");
			RedisModulesCommands<String, String> sync = redis.sync();
			Map<String, String> hash1 = new HashMap<>();
			hash1.put("field1", "value1");
			hash1.put("field2", "value2");
			sync.hmset("hash:1", hash1);
			Map<String, String> hash2 = new HashMap<>();
			hash2.put("field2", "value2");
			sync.hmset("hash:2", hash2);
			execute("export-postgresql", redis, r -> configureExportCommand(r, POSTGRESQL));
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

	@ParameterizedTest
	@RedisTestContextsSource
	void testImport(RedisTestContext redis) throws Exception {
		execute("import-postgresql", redis, r -> configureImportCommand(r, POSTGRESQL));
		try (Statement statement = connection.createStatement()) {
			statement.execute("SELECT COUNT(*) AS count FROM orders");
			RedisModulesCommands<String, String> sync = redis.sync();
			List<String> keys = sync.keys("order:*");
			ResultSet resultSet = statement.getResultSet();
			resultSet.next();
			Assertions.assertEquals(resultSet.getLong("count"), keys.size());
			Map<String, String> order = sync.hgetall("order:10248");
			Assertions.assertEquals("10248", order.get("order_id"));
			Assertions.assertEquals("VINET", order.get("customer_id"));
		}
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void testImportNoop(RedisTestContext redis) throws Exception {
		execute("import-postgresql-noop", redis, r -> configureImportCommand(r, POSTGRESQL));
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void testMultiThreadedImport(RedisTestContext redis) throws Exception {
		execute("import-postgresql-multithreaded", redis, r -> configureImportCommand(r, POSTGRESQL));
		try (Statement statement = connection.createStatement()) {
			statement.execute("SELECT COUNT(*) AS count FROM orders");
			RedisModulesCommands<String, String> sync = redis.sync();
			List<String> keys = sync.keys("order:*");
			ResultSet resultSet = statement.getResultSet();
			resultSet.next();
			Assertions.assertEquals(resultSet.getLong("count"), keys.size());
			Map<String, String> order = sync.hgetall("order:10248");
			Assertions.assertEquals("10248", order.get("order_id"));
			Assertions.assertEquals("VINET", order.get("customer_id"));
		}
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void testImportSet(RedisTestContext redis) throws Exception {
		execute("import-postgresql-set", redis, r -> configureImportCommand(r, POSTGRESQL));
		try (Statement statement = connection.createStatement()) {
			statement.execute("SELECT * FROM orders");
			ResultSet resultSet = statement.getResultSet();
			RedisModulesCommands<String, String> sync = redis.sync();
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
