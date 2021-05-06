package com.redislabs.riot.db;

import com.redislabs.riot.AbstractExportCommand;
import com.redislabs.riot.processor.DataStructureItemProcessor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;

import javax.sql.DataSource;
import java.util.Map;

@Slf4j
@Data
@EqualsAndHashCode(callSuper = true)
@Command(name = "export", description = "Export to a database")
public class DatabaseExportCommand extends AbstractExportCommand<Map<String, Object>> {

    @CommandLine.Parameters(arity = "1", description = "SQL INSERT statement.", paramLabel = "SQL")
    private String sql;
    @Mixin
    private DataSourceOptions dataSourceOptions = DataSourceOptions.builder().build();
    @Mixin
    private DatabaseExportOptions exportOptions = DatabaseExportOptions.builder().build();

    @Override
    protected Flow flow(StepBuilderFactory stepBuilderFactory) throws Exception {
        log.debug("Creating data source: {}", dataSourceOptions);
        DataSource dataSource = dataSourceOptions.dataSource();
        String name = dataSource.getConnection().getMetaData().getDatabaseProductName();
        log.debug("Creating {} database writer: {}", name, exportOptions);
        JdbcBatchItemWriterBuilder<Map<String, Object>> builder = new JdbcBatchItemWriterBuilder<>();
        builder.itemSqlParameterSourceProvider(MapSqlParameterSource::new);
        builder.dataSource(dataSource);
        builder.sql(sql);
        builder.assertUpdates(exportOptions.isAssertUpdates());
        JdbcBatchItemWriter<Map<String, Object>> writer = builder.build();
        writer.afterPropertiesSet();
        DataStructureItemProcessor processor = DataStructureItemProcessor.builder().keyRegex(exportOptions.getKeyRegex()).build();
        return flow(step(stepBuilderFactory, processor, writer).build());
    }

}
