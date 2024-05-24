package com.redis.riot.db;

import java.util.Map;

import org.springframework.batch.item.database.AbstractCursorItemReader;
import org.springframework.batch.item.database.builder.JdbcCursorItemReaderBuilder;
import org.springframework.jdbc.core.ColumnMapRowMapper;

import picocli.CommandLine.Option;

public class DatabaseReaderArgs extends DatabaseArgs {

	public static final int DEFAULT_FETCH_SIZE = AbstractCursorItemReader.VALUE_NOT_SET;
	public static final int DEFAULT_MAX_RESULT_SET_ROWS = AbstractCursorItemReader.VALUE_NOT_SET;
	public static final int DEFAULT_QUERY_TIMEOUT = AbstractCursorItemReader.VALUE_NOT_SET;

	@Option(names = "--max", description = "Max number of rows to import.", paramLabel = "<count>")
	private int maxItemCount;

	@Option(names = "--fetch", description = "Number of rows to return with each fetch.", paramLabel = "<size>")
	private int fetchSize = DEFAULT_FETCH_SIZE;

	@Option(names = "--rows", description = "Max number of rows the ResultSet can contain.", paramLabel = "<count>")
	private int maxRows = DEFAULT_MAX_RESULT_SET_ROWS;

	@Option(names = "--query-timeout", description = "The time in milliseconds for the query to timeout.", paramLabel = "<ms>")
	private int queryTimeout = DEFAULT_QUERY_TIMEOUT;

	@Option(names = "--shared-connection", description = "Use same connection for cursor and other processing.", hidden = true)
	private boolean useSharedExtendedConnection;

	@Option(names = "--verify", description = "Verify position of result set after row mapper.", hidden = true)
	private boolean verifyCursorPosition;

	public JdbcCursorItemReaderBuilder<Map<String, Object>> reader() {
		JdbcCursorItemReaderBuilder<Map<String, Object>> builder = new JdbcCursorItemReaderBuilder<>();
		builder.saveState(false);
		builder.dataSource(dataSource());
		builder.rowMapper(new ColumnMapRowMapper());
		builder.fetchSize(fetchSize);
		builder.maxRows(maxRows);
		builder.queryTimeout(queryTimeout);
		builder.useSharedExtendedConnection(useSharedExtendedConnection);
		builder.verifyCursorPosition(verifyCursorPosition);
		if (maxItemCount > 0) {
			builder.maxItemCount(maxItemCount);
		}
		return builder;
	}

	public int getMaxItemCount() {
		return maxItemCount;
	}

	public void setMaxItemCount(int maxItemCount) {
		this.maxItemCount = maxItemCount;
	}

	public int getFetchSize() {
		return fetchSize;
	}

	public void setFetchSize(int fetchSize) {
		this.fetchSize = fetchSize;
	}

	/**
	 * The max number of rows the {@link java.sql.ResultSet} can contain
	 * 
	 * @return
	 */
	public int getMaxRows() {
		return maxRows;
	}

	public void setMaxRows(int maxResultSetRows) {
		this.maxRows = maxResultSetRows;
	}

	public int getQueryTimeout() {
		return queryTimeout;
	}

	public void setQueryTimeout(int queryTimeout) {
		this.queryTimeout = queryTimeout;
	}

	public boolean isUseSharedExtendedConnection() {
		return useSharedExtendedConnection;
	}

	public void setUseSharedExtendedConnection(boolean useSharedExtendedConnection) {
		this.useSharedExtendedConnection = useSharedExtendedConnection;
	}

	public boolean isVerifyCursorPosition() {
		return verifyCursorPosition;
	}

	public void setVerifyCursorPosition(boolean verifyCursorPosition) {
		this.verifyCursorPosition = verifyCursorPosition;
	}

}
