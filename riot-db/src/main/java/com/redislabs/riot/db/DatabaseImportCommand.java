package com.redislabs.riot.db;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import javax.sql.DataSource;

import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.item.database.builder.JdbcCursorItemReaderBuilder;
import org.springframework.batch.item.redis.support.Transfer;
import org.springframework.jdbc.core.ColumnMapRowMapper;

import com.redislabs.riot.AbstractImportCommand;

import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

@Command(name = "import", aliases = { "i" }, description = "Import from a database")
public class DatabaseImportCommand extends AbstractImportCommand<Map<String, Object>, Map<String, Object>> {

    @Parameters(arity = "1", description = "SQL SELECT statement", paramLabel = "SQL")
    private String sql;
    @Mixin
    private DatabaseOptions options = new DatabaseOptions();
    @Option(names = "--fetch", description = "Number of rows to return with each fetch", paramLabel = "<size>")
    private Integer fetchSize;
    @Option(names = "--rows", description = "Max number of rows the ResultSet can contain", paramLabel = "<count>")
    private Integer maxRows;
    @Option(names = "--query-timeout", description = "The time in milliseconds for the query to timeout", paramLabel = "<ms>")
    private Integer queryTimeout;
    @Option(names = "--shared-connection", description = "Use same conn for cursor and other processing", hidden = true)
    private boolean useSharedExtendedConnection;
    @Option(names = "--verify", description = "Verify position of ResultSet after RowMapper", hidden = true)
    private boolean verifyCursorPosition;

    @Override
    protected List<Transfer<Map<String, Object>, Map<String, Object>>> transfers() throws Exception {
	DataSource dataSource = options.dataSource();
	JdbcCursorItemReaderBuilder<Map<String, Object>> builder = new JdbcCursorItemReaderBuilder<>();
	builder.dataSource(dataSource);
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
	reader.setName(options.name(dataSource));
	reader.afterPropertiesSet();
	return Collections.singletonList(transfer(reader, mapProcessor(), writer()));
    }

}
