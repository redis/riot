package com.redis.riot.db;

import java.util.Map;
import java.util.Optional;

import org.springframework.batch.item.database.builder.JdbcCursorItemReaderBuilder;

import picocli.CommandLine.Option;

public class DatabaseImportOptions {

	@Option(names = "--fetch", description = "Number of rows to return with each fetch.", paramLabel = "<size>")
	private Optional<Integer> fetchSize = Optional.empty();
	@Option(names = "--rows", description = "Max number of rows the ResultSet can contain.", paramLabel = "<count>")
	private Optional<Integer> maxRows = Optional.empty();
	@Option(names = "--query-timeout", description = "The time in milliseconds for the query to timeout.", paramLabel = "<ms>")
	private Optional<Integer> queryTimeout = Optional.empty();
	@Option(names = "--shared-connection", description = "Use same connection for cursor and other processing.", hidden = true)
	private boolean useSharedExtendedConnection;
	@Option(names = "--verify", description = "Verify position of result set after row mapper.", hidden = true)
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
		fetchSize.ifPresent(builder::fetchSize);
		maxRows.ifPresent(builder::maxRows);
		queryTimeout.ifPresent(builder::queryTimeout);
		builder.useSharedExtendedConnection(useSharedExtendedConnection);
		builder.verifyCursorPosition(verifyCursorPosition);
	}

	@Override
	public String toString() {
		return "DatabaseImportOptions [fetchSize=" + fetchSize + ", maxRows=" + maxRows + ", queryTimeout="
				+ queryTimeout + ", useSharedExtendedConnection=" + useSharedExtendedConnection
				+ ", verifyCursorPosition=" + verifyCursorPosition + "]";
	}

}
