package com.redislabs.riot.cli.db;

import lombok.Getter;
import picocli.CommandLine.Option;

public class DatabaseImportOptions extends DatabaseOptions {

	@Getter
	@Option(names = "--sql", description = "SELECT statement", paramLabel = "<sql>")
	private String sql;
	@Getter
	@Option(names = "--fetch", description = "Number of rows to return with each fetch", paramLabel = "<size>")
	private Integer fetchSize;
	@Getter
	@Option(names = "--rows", description = "Max number of rows the ResultSet can contain", paramLabel = "<count>")
	private Integer maxRows;
	@Getter
	@Option(names = "--query-timeout", description = "The time in milliseconds for the query to timeout", paramLabel = "<ms>")
	private Integer queryTimeout;
	@Getter
	@Option(names = "--shared-connection", description = "Use same conn for cursor and other processing")
	private boolean useSharedExtendedConnection;
	@Getter
	@Option(names = "--verify", description = "Verify position of ResultSet after RowMapper")
	private boolean verifyCursorPosition;

}
