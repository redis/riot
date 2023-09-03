package com.redis.riot.cli;

import com.redis.riot.core.AbstractMapImport;
import com.redis.riot.core.StepBuilder;
import com.redis.riot.db.DatabaseImport;
import com.redis.spring.batch.util.BatchUtils;

import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

@Command(name = "db-import", description = "Import from a relational database.")
public class DatabaseImportCommand extends AbstractImportCommand {

    @Parameters(arity = "1", description = "SQL SELECT statement", paramLabel = "SQL")
    private String sql;

    @ArgGroup(exclusive = false, heading = "JDBC driver options%n")
    private DataSourceArgs dataSourceArgs = new DataSourceArgs();

    @Option(names = "--max", description = "Max number of rows to import.", paramLabel = "<count>")
    private int maxItemCount;

    @Option(names = "--fetch", description = "Number of rows to return with each fetch.", paramLabel = "<size>")
    private int fetchSize = DatabaseImport.DEFAULT_FETCH_SIZE;

    @Option(names = "--rows", description = "Max number of rows the ResultSet can contain.", paramLabel = "<count>")
    private int maxResultSetRows = DatabaseImport.DEFAULT_MAX_RESULT_SET_ROWS;

    @Option(names = "--query-timeout", description = "The time in milliseconds for the query to timeout.", paramLabel = "<ms>")
    private int queryTimeout = DatabaseImport.DEFAULT_QUERY_TIMEOUT;

    @Option(names = "--shared-connection", description = "Use same connection for cursor and other processing.", hidden = true)
    private boolean useSharedExtendedConnection;

    @Option(names = "--verify", description = "Verify position of result set after row mapper.", hidden = true)
    private boolean verifyCursorPosition;

    @Override
    protected AbstractMapImport getMapImportExecutable() {
        DatabaseImport executable = new DatabaseImport(redisClient(), sql);
        executable.setDataSourceOptions(dataSourceArgs.dataSourceOptions());
        executable.setFetchSize(fetchSize);
        executable.setMaxItemCount(maxItemCount);
        executable.setMaxResultSetRows(maxResultSetRows);
        return executable;
    }

    @Override
    protected long size(StepBuilder<?, ?> step) {
        return BatchUtils.SIZE_UNKNOWN;
    }

}
