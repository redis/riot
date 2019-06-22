package com.redislabs.riot.cli;

import java.util.Map;

import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.item.database.builder.JdbcCursorItemReaderBuilder;
import org.springframework.jdbc.core.ColumnMapRowMapper;

import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "db", description = " database")
public class DatabaseReaderCommand extends AbstractReaderCommand {

	@ArgGroup(exclusive = false, heading = "Database connection%n")
	private DatabaseConnectionOptions connection = new DatabaseConnectionOptions();
	@Option(names = "--sql", required = true, description = "The query to be executed for this reader.", paramLabel = "<stmt>")
	private String sql;
	@Option(names = "--fetch", description = "A hint to the driver as to how many rows to return with each fetch.", paramLabel = "<size>")
	private Integer fetchSize;
	@Option(names = "--rows", description = "The max number of rows the ResultSet can contain.", paramLabel = "<count>")
	private Integer maxRows;
	@Option(names = "--timeout", description = "The time in milliseconds for the query to timeout.", paramLabel = "<millis>")
	private Integer queryTimeout;
	@Option(names = "--use-shared-extended-connection", description = "Connection used for cursor to be used by all other processing, therefore part of the same transaction.")
	private boolean useSharedExtendedConnection;
	@Option(names = "--verify", description = "Verify position of ResultSet after RowMapper.")
	private boolean verifyCursorPosition;

	@Override
	public JdbcCursorItemReader<Map<String, Object>> reader() {
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
		return builder.build();
	}

	@Override
	public String getSourceDescription() {
		return "database query \"" + sql + "\"";
	}

}
