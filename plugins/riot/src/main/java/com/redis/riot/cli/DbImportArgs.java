package com.redis.riot.cli;

import com.redis.riot.db.DatabaseImport;

import picocli.CommandLine.Option;

public class DbImportArgs extends DbArgs {

    @Option(names = "--max", description = "Max number of rows to import.", paramLabel = "<count>")
    int maxItemCount;

    @Option(names = "--fetch", description = "Number of rows to return with each fetch.", paramLabel = "<size>")
    int fetchSize = DatabaseImport.DEFAULT_FETCH_SIZE;

    @Option(names = "--rows", description = "Max number of rows the ResultSet can contain.", paramLabel = "<count>")
    int maxResultSetRows = DatabaseImport.DEFAULT_MAX_RESULT_SET_ROWS;

    @Option(names = "--query-timeout", description = "The time in milliseconds for the query to timeout.", paramLabel = "<ms>")
    int queryTimeout = DatabaseImport.DEFAULT_QUERY_TIMEOUT;

    @Option(names = "--shared-connection", description = "Use same connection for cursor and other processing.", hidden = true)
    boolean useSharedExtendedConnection;

    @Option(names = "--verify", description = "Verify position of result set after row mapper.", hidden = true)
    boolean verifyCursorPosition;

}
