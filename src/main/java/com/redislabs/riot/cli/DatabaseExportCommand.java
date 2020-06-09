package com.redislabs.riot.cli;

import com.redislabs.riot.processor.KeyValueItemProcessor;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.redis.support.TypeKeyValue;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import picocli.CommandLine;

import java.util.Map;

@CommandLine.Command(name = "db", description = "Export to database")
public class DatabaseExportCommand extends AbstractExportCommand<Map<String, Object>> {

    @CommandLine.Mixin
    private DatabaseOptions options = new DatabaseOptions();
    @CommandLine.Option(names = "--no-assert-updates", description = "Disable insert verification")
    private boolean noAssertUpdates;

    @Override
    protected JdbcBatchItemWriter<Map<String, Object>> writer() {
        JdbcBatchItemWriterBuilder<Map<String, Object>> builder = new JdbcBatchItemWriterBuilder<>();
        builder.itemSqlParameterSourceProvider(MapSqlParameterSource::new);
        builder.dataSource(options.getDataSource());
        builder.sql(options.getSql());
        builder.assertUpdates(!noAssertUpdates);
        JdbcBatchItemWriter<Map<String, Object>> writer = builder.build();
        writer.afterPropertiesSet();
        return writer;
    }

    @Override
    protected ItemProcessor<TypeKeyValue<String>, Map<String, Object>> processor() {
        return KeyValueItemProcessor.builder().build();
    }
}
