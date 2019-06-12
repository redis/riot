package com.redislabs.riot.cli.in;

import java.util.Map;

import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.item.database.builder.JdbcCursorItemReaderBuilder;
import org.springframework.jdbc.core.ColumnMapRowMapper;

import com.redislabs.riot.cli.DatabaseConnectionOptions;

import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Option;

@Command(name = "db", description = "Import from a database")
public class DatabaseImport extends AbstractImportReaderCommand {

	@Mixin
	private DatabaseConnectionOptions connection = new DatabaseConnectionOptions();
	@Option(names = "--sql", description = "The query to be executed for this reader.", required = true)
	private String sql;
	@Option(names = "--fetch", description = "A hint to the driver as to how many rows to return with each fetch.")
	private Integer fetchSize;
	@Option(names = "--rows", description = "The max number of rows the ResultSet can contain.")
	private Integer maxRows;
	@Option(names = "--timeout", description = "The time in milliseconds for the query to timeout.")
	private Integer queryTimeout;
	@Option(names = "--use-shared-extended-connection", description = "Indicates that the connection used for the cursor is being used by all other processing, therefore part of the same transaction.")
	private boolean useSharedExtendedConnection;
	@Option(names = "--verify", description = "Indicates if the reader should verify the current position of the ResultSet after being passed to the RowMapper.")
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
