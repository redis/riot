package com.redislabs.riot.db;

import com.redislabs.riot.AbstractImportCommand;
import com.redislabs.riot.KeyValueProcessingOptions;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.item.database.builder.JdbcCursorItemReaderBuilder;
import org.springframework.jdbc.core.ColumnMapRowMapper;
import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;

import javax.sql.DataSource;
import java.util.Map;

@Command(name = "import", aliases = {"i"}, description = "Import from a database")
public class DatabaseImportCommand extends AbstractImportCommand<Map<String, Object>, Map<String, Object>> {

    @Mixin
    private DatabaseImportOptions options = new DatabaseImportOptions();
    @Mixin
    private KeyValueProcessingOptions processingOptions = new KeyValueProcessingOptions();

    @Override
    protected Flow flow() throws Exception {
        DataSource dataSource = options.dataSource();
        JdbcCursorItemReaderBuilder<Map<String, Object>> builder = new JdbcCursorItemReaderBuilder<>();
        builder.saveState(false);
        builder.dataSource(dataSource);
        if (options.getFetchSize() != null) {
            builder.fetchSize(options.getFetchSize());
        }
        if (options.getMaxRows() != null) {
            builder.maxRows(options.getMaxRows());
        }
        builder.name("database-reader");
        if (options.getQueryTimeout() != null) {
            builder.queryTimeout(options.getQueryTimeout());
        }
        builder.rowMapper(new ColumnMapRowMapper());
        builder.sql(options.getSql());
        builder.useSharedExtendedConnection(options.isUseSharedExtendedConnection());
        builder.verifyCursorPosition(options.isVerifyCursorPosition());
        JdbcCursorItemReader<Map<String, Object>> reader = builder.build();
        reader.afterPropertiesSet();
        String name = dataSource.getConnection().getMetaData().getDatabaseProductName();
        return flow(step("Importing from " + name, reader).build());
    }

    @Override
    protected ItemProcessor<Map<String, Object>, Map<String, Object>> processor() {
        return processingOptions.processor(connection());
    }
}
