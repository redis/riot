package com.redis.riot;

import java.io.IOException;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

@EnabledOnOs(value = OS.LINUX)
class PostgresDeltaTests extends DbTests {

	private static final DockerImageName postgresImage = DockerImageName.parse(PostgreSQLContainer.IMAGE)
			.withTag(PostgreSQLContainer.DEFAULT_TAG);
	private static final PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>(postgresImage);

	@Override
	protected JdbcDatabaseContainer<?> getJdbcDatabaseContainer() {
		return postgres;
	}

	@BeforeAll
	void setupPostgres() throws SQLException, IOException {
		executeScript("db/postgres-delta.sql");
	}

	@Test
	void importDelta(TestInfo info) throws Exception {
		try (Statement statement = dbConnection.createStatement()) {
			for (int index = 0; index < 10; index++) {
				statement.execute("INSERT INTO orders (order_date) VALUES (CURRENT_TIMESTAMP - INTERVAL '5 seconds')");
			}
		}
	}

}
