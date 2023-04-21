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

import com.redis.testcontainers.RedisServer;
import com.redis.testcontainers.RedisStackContainer;

import picocli.CommandLine;
import picocli.CommandLine.ParseResult;

abstract class AbstractDatabaseTests extends AbstractTestBase {

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
	protected ParseResult parse(RiotCommandLine riotCommandLine, String filename) throws Exception {
		ParseResult parseResult = super.parse(riotCommandLine, filename);
		CommandLine commandLine = parseResult.subcommand().commandSpec().commandLine();
		DataSourceOptions dataSourceOptions;
		if (commandLine.getCommand() instanceof DatabaseExportCommand) {
			DatabaseExportCommand exportCommand = commandLine.getCommand();
			dataSourceOptions = exportCommand.getDataSourceOptions();
		} else {
			DatabaseImportCommand importCommand = (DatabaseImportCommand) commandLine.getCommandSpec().parent()
					.userObject();
			dataSourceOptions = importCommand.getDataSourceOptions();
		}
		JdbcDatabaseContainer<?> container = getJdbcDatabaseContainer();
		dataSourceOptions.setUrl(container.getJdbcUrl());
		dataSourceOptions.setUsername(container.getUsername());
		dataSourceOptions.setPassword(container.getPassword());
		return parseResult;
	}

}
