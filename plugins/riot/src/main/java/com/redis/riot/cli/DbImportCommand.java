package com.redis.riot.cli;

import com.redis.riot.core.AbstractMapImport;
import com.redis.riot.db.DatabaseImport;

import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

@Command(name = "db-import", description = "Import from a relational database.")
public class DbImportCommand extends AbstractImportCommand {

	@Parameters(arity = "1", description = "SQL SELECT statement", paramLabel = "SQL")
	private String sql;

	@ArgGroup(exclusive = false)
	private DbArgs dbArgs = new DbArgs();

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
	protected AbstractMapImport importRunnable() {
		DatabaseImport runnable = new DatabaseImport();
		runnable.setSql(sql);
		runnable.setDataSourceOptions(dbArgs.dataSourceOptions());
		runnable.setFetchSize(fetchSize);
		runnable.setMaxItemCount(maxItemCount);
		runnable.setMaxResultSetRows(maxResultSetRows);
		return runnable;
	}

	public String getSql() {
		return sql;
	}

	public void setSql(String sql) {
		this.sql = sql;
	}

	public DbArgs getDbArgs() {
		return dbArgs;
	}

	public void setDbArgs(DbArgs dbArgs) {
		this.dbArgs = dbArgs;
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

	public int getMaxResultSetRows() {
		return maxResultSetRows;
	}

	public void setMaxResultSetRows(int maxResultSetRows) {
		this.maxResultSetRows = maxResultSetRows;
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
