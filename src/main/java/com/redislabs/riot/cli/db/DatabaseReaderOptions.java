package com.redislabs.riot.cli.db;

import java.util.Map;

import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.item.database.builder.JdbcCursorItemReaderBuilder;
import org.springframework.jdbc.core.ColumnMapRowMapper;

import lombok.Data;
import picocli.CommandLine.Option;

public @Data class DatabaseReaderOptions extends DatabaseOptions {

	@Option(required = true, names = "--sql", description = "SELECT statement", paramLabel = "<sql>")
	private String sql;
	@Option(names = "--fetch", description = "# rows to return with each fetch", paramLabel = "<size>")
	private Integer fetchSize;
	@Option(names = "--rows", description = "The max number of rows the ResultSet can contain", paramLabel = "<count>")
	private Integer maxRows;
	@Option(names = "--timeout", description = "The time in milliseconds for the query to timeout", paramLabel = "<ms>")
	private Integer queryTimeout;
	@Option(names = "--shared-connection", description = "Use same conn for cursor and other processing")
	private boolean useSharedExtendedConnection;
	@Option(names = "--verify", description = "Verify position of ResultSet after RowMapper")
	private boolean verifyCursorPosition;

	public boolean isSet() {
		return sql != null;
	}

	public JdbcCursorItemReader<Map<String, Object>> reader() throws Exception {
		JdbcCursorItemReaderBuilder<Map<String, Object>> builder = new JdbcCursorItemReaderBuilder<Map<String, Object>>();
		builder.dataSource(dataSource());
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

}
