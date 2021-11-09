package com.redis.riot.db;

import com.redis.riot.AbstractRiotIntegrationTests;
import org.springframework.boot.autoconfigure.jdbc.DataSourceProperties;
import org.testcontainers.containers.JdbcDatabaseContainer;
import picocli.CommandLine;

import javax.sql.DataSource;

public abstract class AbstractDatabaseTests extends AbstractRiotIntegrationTests {

	@Override
	protected RiotDb app() {
		return new RiotDb();
	}

	@SuppressWarnings("rawtypes")
	protected static DataSource dataSource(JdbcDatabaseContainer container) {
		DataSourceProperties properties = new DataSourceProperties();
		properties.setUrl(container.getJdbcUrl());
		properties.setUsername(container.getUsername());
		properties.setPassword(container.getPassword());
		return properties.initializeDataSourceBuilder().build();
	}

	protected void configureExportCommand(CommandLine.ParseResult parseResult, JdbcDatabaseContainer<?> container) {
		DatabaseExportCommand exportCommand = parseResult.subcommand().commandSpec().commandLine().getCommand();
		configure(exportCommand.getDataSourceOptions(), container);
	}

	private void configure(DataSourceOptions dataSourceOptions, JdbcDatabaseContainer<?> container) {
		dataSourceOptions.setUrl(container.getJdbcUrl());
		dataSourceOptions.setUsername(container.getUsername());
		dataSourceOptions.setPassword(container.getPassword());
	}

	protected void configureImportCommand(CommandLine.ParseResult parseResult, JdbcDatabaseContainer<?> container) {
		DatabaseImportCommand importCommand = (DatabaseImportCommand) parseResult.subcommand().commandSpec()
				.commandLine().getCommandSpec().parent().userObject();
		configure(importCommand.getDataSourceOptions(), container);
	}
}
