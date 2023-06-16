package com.redis.riot.cli.db;

import org.springframework.batch.item.database.AbstractCursorItemReader;

import picocli.CommandLine.Mixin;
import picocli.CommandLine.Option;

public class DbImportOptions {

	public static final int DEFAULT_FETCH_SIZE = AbstractCursorItemReader.VALUE_NOT_SET;
	public static final int DEFAULT_MAX_RESULT_SET_ROWS = AbstractCursorItemReader.VALUE_NOT_SET;
	public static final int DEFAULT_QUERY_TIMEOUT = AbstractCursorItemReader.VALUE_NOT_SET;

	@Mixin
	private DataSourceOptions dataSourceOptions = new DataSourceOptions();
	@Option(names = "--max", description = "Max number of rows to import.", paramLabel = "<count>")
	private int maxItemCount;
	@Option(names = "--fetch", description = "Number of rows to return with each fetch.", paramLabel = "<size>")
	private int fetchSize = DEFAULT_FETCH_SIZE;
	@Option(names = "--rows", description = "Max number of rows the ResultSet can contain.", paramLabel = "<count>")
	private int maxResultSetRows = DEFAULT_MAX_RESULT_SET_ROWS;
	@Option(names = "--query-timeout", description = "The time in milliseconds for the query to timeout.", paramLabel = "<ms>")
	private int queryTimeout = DEFAULT_QUERY_TIMEOUT;
	@Option(names = "--shared-connection", description = "Use same connection for cursor and other processing.", hidden = true)
	private boolean useSharedExtendedConnection;
	@Option(names = "--verify", description = "Verify position of result set after row mapper.", hidden = true)
	private boolean verifyCursorPosition;

	public DataSourceOptions getDataSourceOptions() {
		return dataSourceOptions;
	}

	public int getMaxItemCount() {
		return maxItemCount;
	}

	public int getFetchSize() {
		return fetchSize;
	}

	public int getMaxResultSetRows() {
		return maxResultSetRows;
	}

	public int getQueryTimeout() {
		return queryTimeout;
	}

	public boolean isUseSharedExtendedConnection() {
		return useSharedExtendedConnection;
	}

	public boolean isVerifyCursorPosition() {
		return verifyCursorPosition;
	}

	public void setDataSourceOptions(DataSourceOptions dataSourceOptions) {
		this.dataSourceOptions = dataSourceOptions;
	}

	public void setMaxItemCount(int maxItemCount) {
		this.maxItemCount = maxItemCount;
	}

	public void setFetchSize(int fetchSize) {
		this.fetchSize = fetchSize;
	}

	public void setMaxResultSetRows(int rows) {
		this.maxResultSetRows = rows;
	}

	public void setQueryTimeout(int queryTimeout) {
		this.queryTimeout = queryTimeout;
	}

	public void setUseSharedExtendedConnection(boolean useSharedExtendedConnection) {
		this.useSharedExtendedConnection = useSharedExtendedConnection;
	}

	public void setVerifyCursorPosition(boolean verifyCursorPosition) {
		this.verifyCursorPosition = verifyCursorPosition;
	}

	@Override
	public String toString() {
		return "DatabaseImportOptions [fetchSize=" + fetchSize + ", maxResultSetRows=" + maxResultSetRows
				+ ", queryTimeout=" + queryTimeout + ", useSharedExtendedConnection=" + useSharedExtendedConnection
				+ ", verifyCursorPosition=" + verifyCursorPosition + "]";
	}

}
