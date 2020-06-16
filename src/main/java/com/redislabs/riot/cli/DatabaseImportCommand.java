package com.redislabs.riot.cli;

import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.item.database.builder.JdbcCursorItemReaderBuilder;
import org.springframework.jdbc.core.ColumnMapRowMapper;
import picocli.CommandLine;

import java.util.Map;

@CommandLine.Command(name = "db", description = "Import database")
public class DatabaseImportCommand extends AbstractImportCommand<Map<String, Object>> {

    @CommandLine.Mixin
    private DatabaseOptions options = new DatabaseOptions();
    @CommandLine.Option(names = "--fetch", description = "Number of rows to return with each fetch", paramLabel = "<size>")
    private Integer fetchSize;
    @CommandLine.Option(names = "--rows", description = "Max number of rows the ResultSet can contain", paramLabel = "<count>")
    private Integer maxRows;
    @CommandLine.Option(names = "--query-timeout", description = "The time in milliseconds for the query to timeout", paramLabel = "<ms>")
    private Integer queryTimeout;
    @CommandLine.Option(names = "--shared-connection", description = "Use same conn for cursor and other processing", hidden = true)
    private boolean useSharedExtendedConnection;
    @CommandLine.Option(names = "--verify", description = "Verify position of ResultSet after RowMapper", hidden = true)
    private boolean verifyCursorPosition;

    @Override
    protected JdbcCursorItemReader<Map<String, Object>> reader() throws Exception {
        JdbcCursorItemReaderBuilder<Map<String, Object>> builder = new JdbcCursorItemReaderBuilder<>();
        builder.dataSource(options.getDataSource());
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
        builder.sql(options.getSql());
        builder.useSharedExtendedConnection(useSharedExtendedConnection);
        builder.verifyCursorPosition(verifyCursorPosition);
        JdbcCursorItemReader<Map<String, Object>> reader = builder.build();
        reader.afterPropertiesSet();
        return reader;
    }

    @Override
    protected ItemProcessor<Map<String, Object>, Object> processor() {
        return getParentCommand().objectMapProcessor();
    }
}
