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

@Command(name = "import", description = "Import from a database")
public class DatabaseImportCommand extends AbstractImportCommand<Map<String, Object>, Map<String, Object>> {

    @Mixin
    private DataSourceOptions options = DataSourceOptions.builder().build();
    @Mixin
    private DatabaseImportOptions importOptions = DatabaseImportOptions.builder().build();
    @Mixin
    private KeyValueProcessingOptions processingOptions = KeyValueProcessingOptions.builder().build();

    @Override
    protected Flow flow() throws Exception {
        DataSource dataSource = options.dataSource();
        JdbcCursorItemReaderBuilder<Map<String, Object>> builder = new JdbcCursorItemReaderBuilder<>();
        builder.saveState(false);
        builder.dataSource(dataSource);
        if (importOptions.getFetchSize() != null) {
            builder.fetchSize(importOptions.getFetchSize());
        }
        if (importOptions.getMaxRows() != null) {
            builder.maxRows(importOptions.getMaxRows());
        }
        builder.name("database-reader");
        if (importOptions.getQueryTimeout() != null) {
            builder.queryTimeout(importOptions.getQueryTimeout());
        }
        builder.rowMapper(new ColumnMapRowMapper());
        builder.sql(importOptions.getSql());
        builder.useSharedExtendedConnection(importOptions.isUseSharedExtendedConnection());
        builder.verifyCursorPosition(importOptions.isVerifyCursorPosition());
        JdbcCursorItemReader<Map<String, Object>> reader = builder.build();
        reader.afterPropertiesSet();
        String name = dataSource.getConnection().getMetaData().getDatabaseProductName();
        return flow(step(name + "-db-import-step", "Importing from " + name, reader).build());
    }

    @Override
    protected ItemProcessor<Map<String, Object>, Map<String, Object>> processor() {
        return processingOptions.processor(connection);
    }
}
