package com.redislabs.riot.db;

import com.redislabs.riot.AbstractExportCommand;
import com.redislabs.riot.processor.DataStructureMapItemProcessor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;

import java.util.Map;

@Slf4j
@Command(name = "export", description = "Export to a database")
public class DatabaseExportCommand extends AbstractExportCommand<Map<String, Object>> {

    @Mixin
    private DataSourceOptions dataSourceOptions = DataSourceOptions.builder().build();
    @Mixin
    private DatabaseExportOptions exportOptions = DatabaseExportOptions.builder().build();

    @Override
    protected Flow flow() throws Exception {
        JdbcBatchItemWriterBuilder<Map<String, Object>> builder = new JdbcBatchItemWriterBuilder<>();
        builder.itemSqlParameterSourceProvider(MapSqlParameterSource::new);
        log.info("Creating data source {}", dataSourceOptions);
        builder.dataSource(dataSourceOptions.dataSource());
        builder.sql(exportOptions.getSql());
        builder.assertUpdates(exportOptions.isAssertUpdates());
        JdbcBatchItemWriter<Map<String, Object>> writer = builder.build();
        writer.afterPropertiesSet();
        DataStructureMapItemProcessor processor = DataStructureMapItemProcessor.builder().keyRegex(exportOptions.getKeyRegex()).build();
        return flow(step(processor, writer).build());
    }

}
