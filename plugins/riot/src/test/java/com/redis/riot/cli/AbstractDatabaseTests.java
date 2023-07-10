package com.redis.riot.cli;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.SQLException;

import javax.sql.DataSource;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.springframework.boot.autoconfigure.jdbc.DataSourceProperties;
import org.testcontainers.containers.JdbcDatabaseContainer;

import com.redis.riot.cli.common.DataSourceOptions;
import com.redis.testcontainers.RedisServer;
import com.redis.testcontainers.RedisStackContainer;

import picocli.CommandLine.ParseResult;

abstract class AbstractDatabaseTests extends AbstractTests {

	private static final RedisStackContainer REDIS = new RedisStackContainer(
			RedisStackContainer.DEFAULT_IMAGE_NAME.withTag(RedisStackContainer.DEFAULT_TAG));

	protected abstract JdbcDatabaseContainer<?> getJdbcDatabaseContainer();

	protected Connection databaseConnection;
	protected DataSource dataSource;

	@BeforeAll
	public void setupContainers() throws SQLException {
		JdbcDatabaseContainer<?> container = getJdbcDatabaseContainer();
		container.start();
		DataSourceProperties properties = new DataSourceProperties();
		properties.setUrl(container.getJdbcUrl());
		properties.setUsername(container.getUsername());
		properties.setPassword(container.getPassword());
		dataSource = properties.initializeDataSourceBuilder().build();
		databaseConnection = dataSource.getConnection();
	}

	@AfterAll
	public void teardownContainers() throws SQLException {
		databaseConnection.close();
		getJdbcDatabaseContainer().stop();
	}

	@Override
	protected RedisServer getRedisServer() {
		return REDIS;
	}

	protected void executeScript(String file) throws IOException, SQLException {
		ScriptRunner scriptRunner = new ScriptRunner(databaseConnection);
		scriptRunner.setAutoCommit(false);
		scriptRunner.setStopOnError(true);
		InputStream inputStream = PostgresTests.class.getClassLoader().getResourceAsStream(file);
		if (inputStream == null) {
			throw new FileNotFoundException(file);
		}
		scriptRunner.runScript(new InputStreamReader(inputStream));
	}

	@Override
	protected void configureSubcommand(ParseResult sub) {
		super.configureSubcommand(sub);
		Object command = sub.commandSpec().commandLine().getCommand();
		if (command instanceof DbImport) {
			configure(((DbImport) command).getDbImportOptions().getDataSourceOptions());
		}
		if (command instanceof DbExport) {
			configure(((DbExport) command).getDbExportOptions().getDataSourceOptions());
		}
	}

	private void configure(DataSourceOptions dataSourceOptions) {
		JdbcDatabaseContainer<?> container = getJdbcDatabaseContainer();
		dataSourceOptions.setUrl(container.getJdbcUrl());
		dataSourceOptions.setUsername(container.getUsername());
		dataSourceOptions.setPassword(container.getPassword());
	}

}
