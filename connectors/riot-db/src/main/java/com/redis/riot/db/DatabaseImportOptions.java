package com.redis.riot.db;

import java.util.Map;
import java.util.Optional;

import org.springframework.batch.item.database.builder.JdbcCursorItemReaderBuilder;

import picocli.CommandLine;

public class DatabaseImportOptions {

	@CommandLine.Option(names = "--fetch", description = "Number of rows to return with each fetch.", paramLabel = "<size>")
	private Optional<Integer> fetchSize = Optional.empty();
	@CommandLine.Option(names = "--rows", description = "Max number of rows the ResultSet can contain.", paramLabel = "<count>")
	private Optional<Integer> maxRows = Optional.empty();
	@CommandLine.Option(names = "--query-timeout", description = "The time in milliseconds for the query to timeout.", paramLabel = "<ms>")
	private Optional<Integer> queryTimeout = Optional.empty();
	@CommandLine.Option(names = "--shared-connection", description = "Use same connection for cursor and other processing.", hidden = true)
	private boolean useSharedExtendedConnection;
	@CommandLine.Option(names = "--verify", description = "Verify position of result set after row mapper.", hidden = true)
	private boolean verifyCursorPosition;

	public void setFetchSize(int fetchSize) {
		this.fetchSize = Optional.of(fetchSize);
	}

	public void setMaxRows(int maxRows) {
		this.maxRows = Optional.of(maxRows);
	}

	public void setQueryTimeout(int queryTimeout) {
		this.queryTimeout = Optional.of(queryTimeout);
	}

	public void setUseSharedExtendedConnection(boolean useSharedExtendedConnection) {
		this.useSharedExtendedConnection = useSharedExtendedConnection;
	}

	public void setVerifyCursorPosition(boolean verifyCursorPosition) {
		this.verifyCursorPosition = verifyCursorPosition;
	}

	public void configure(JdbcCursorItemReaderBuilder<Map<String, Object>> builder) {
		if (fetchSize.isPresent()) {
			builder.fetchSize(fetchSize.get());
		}
		if (maxRows.isPresent()) {
			builder.maxRows(maxRows.get());
		}
		if (queryTimeout.isPresent()) {
			builder.queryTimeout(queryTimeout.get());
		}
		builder.useSharedExtendedConnection(useSharedExtendedConnection);
		builder.verifyCursorPosition(verifyCursorPosition);
	}

}
