package com.redis.riot.db;

import picocli.CommandLine;

public class DatabaseImportOptions {

	@CommandLine.Option(names = "--fetch", description = "Number of rows to return with each fetch.", paramLabel = "<size>")
	private Integer fetchSize;
	@CommandLine.Option(names = "--rows", description = "Max number of rows the ResultSet can contain.", paramLabel = "<count>")
	private Integer maxRows;
	@CommandLine.Option(names = "--query-timeout", description = "The time in milliseconds for the query to timeout.", paramLabel = "<ms>")
	private Integer queryTimeout;
	@CommandLine.Option(names = "--shared-connection", description = "Use same connection for cursor and other processing.", hidden = true)
	private boolean useSharedExtendedConnection;
	@CommandLine.Option(names = "--verify", description = "Verify position of result set after row mapper.", hidden = true)
	private boolean verifyCursorPosition;

	public Integer getFetchSize() {
		return fetchSize;
	}

	public Integer getMaxRows() {
		return maxRows;
	}

	public Integer getQueryTimeout() {
		return queryTimeout;
	}

	public boolean isUseSharedExtendedConnection() {
		return useSharedExtendedConnection;
	}

	public boolean isVerifyCursorPosition() {
		return verifyCursorPosition;
	}

	public void setFetchSize(Integer fetchSize) {
		this.fetchSize = fetchSize;
	}

	public void setMaxRows(Integer maxRows) {
		this.maxRows = maxRows;
	}

	public void setQueryTimeout(Integer queryTimeout) {
		this.queryTimeout = queryTimeout;
	}

	public void setUseSharedExtendedConnection(boolean useSharedExtendedConnection) {
		this.useSharedExtendedConnection = useSharedExtendedConnection;
	}

	public void setVerifyCursorPosition(boolean verifyCursorPosition) {
		this.verifyCursorPosition = verifyCursorPosition;
	}

}
