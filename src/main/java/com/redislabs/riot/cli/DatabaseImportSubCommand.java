package com.redislabs.riot.cli;

import java.util.Map;

import org.springframework.batch.item.ItemStreamReader;
import org.springframework.batch.item.database.builder.JdbcCursorItemReaderBuilder;
import org.springframework.boot.autoconfigure.jdbc.DataSourceProperties;
import org.springframework.jdbc.core.ColumnMapRowMapper;
import org.springframework.stereotype.Component;

import com.zaxxer.hikari.HikariDataSource;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Component
@Command(name = "db", description = "Import from a database", sortOptions = false)
public class DatabaseImportSubCommand extends AbstractImportSubCommand {

	@Option(names = { "-d",
			"--driver" }, description = "Fully qualified name of the JDBC driver. Auto-detected based on the URL by default.")
	private String driverClassName;
	@Option(names = { "-u", "--url" }, description = "JDBC URL of the database.", required = true)
	private String url;
	@Option(names = "--username", description = "Login username of the database.")
	private String username;
	@Option(names = "--password", description = "Login password of the database.", interactive = true)
	private String password;
	@Option(names = "--sql", description = "The query to be executed for this reader.", required = true)
	private String sql;
	@Option(names = "--fetch-size", description = "A hint to the driver as to how many rows to return with each fetch.")
	private Integer fetchSize;
	@Option(names = "--max-rows", description = "The max number of rows the ResultSet can contain.")
	private Integer maxRows;
	@Option(names = "--query-timeout", description = "The time in milliseconds for the query to timeout.")
	private Integer queryTimeout;
	@Option(names = "--use-shared-extended-connection", description = "Indicates that the connection used for the cursor is being used by all other processing, therefore part of the same transaction.")
	private boolean useSharedExtendedConnection;
	@Option(names = "--verify-cursor-position", description = "Indicates if the reader should verify the current position of the ResultSet after being passed to the RowMapper.")
	private boolean verifyCursorPosition;

	@Override
	public ItemStreamReader<Map<String, Object>> reader() {
		DataSourceProperties properties = new DataSourceProperties();
		properties.setUrl(url);
		properties.setDriverClassName(driverClassName);
		properties.setUsername(username);
		properties.setPassword(password);
		HikariDataSource dataSource = properties.initializeDataSourceBuilder().type(HikariDataSource.class).build();
		JdbcCursorItemReaderBuilder<Map<String, Object>> builder = new JdbcCursorItemReaderBuilder<Map<String, Object>>();
		builder.dataSource(dataSource);
		if (fetchSize != null) {
			builder.fetchSize(fetchSize);
		}
		if (maxRows != null) {
			builder.maxRows(maxRows);
		}
		builder.name("database-reader");
		if (queryTimeout != null) {
			builder.queryTimeout(queryTimeout);
		}
		builder.rowMapper(new ColumnMapRowMapper());
		builder.sql(sql);
		builder.useSharedExtendedConnection(useSharedExtendedConnection);
		builder.verifyCursorPosition(verifyCursorPosition);
		return builder.build();
	}

	@Override
	public String getSourceDescription() {
		return "database query \"" + sql + "\"";
	}

}
