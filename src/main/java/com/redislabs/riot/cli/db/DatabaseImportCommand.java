package com.redislabs.riot.cli.db;

import java.util.Map;

import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.item.database.builder.JdbcCursorItemReaderBuilder;
import org.springframework.jdbc.core.ColumnMapRowMapper;

import com.redislabs.riot.cli.ImportCommand;

import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Option;

@Command(name = "db-import", description = "Import database into Redis")
public class DatabaseImportCommand extends ImportCommand {

	@Mixin
	private DatabaseConnectionOptions connection = new DatabaseConnectionOptions();
	@Option(names = "--sql", required = true, description = "SELECT statement", paramLabel = "<string>")
	private String sql;
	@Option(names = "--fetch", description = "A hint to the driver as to how many rows to return with each fetch", paramLabel = "<size>")
	private Integer fetchSize;
	@Option(names = "--rows", description = "The max number of rows the ResultSet can contain", paramLabel = "<count>")
	private Integer maxRows;
	@Option(names = "--timeout", description = "The time in milliseconds for the query to timeout", paramLabel = "<millis>")
	private Integer queryTimeout;
	@Option(names = "--shared-connection", description = "Use same connection for cursor and all other processing")
	private boolean useSharedExtendedConnection;
	@Option(names = "--verify", description = "Verify position of ResultSet after RowMapper")
	private boolean verifyCursorPosition;

	@Override
	protected ItemReader<Map<String, Object>> reader() throws Exception {
		JdbcCursorItemReaderBuilder<Map<String, Object>> builder = new JdbcCursorItemReaderBuilder<Map<String, Object>>();
		builder.dataSource(connection.dataSource());
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
		JdbcCursorItemReader<Map<String, Object>> reader = builder.build();
		reader.afterPropertiesSet();
		return reader;
	}

	@Override
	protected String sourceDescription() {
		return String.format("SQL \"%s\"", sql);
	}
}
