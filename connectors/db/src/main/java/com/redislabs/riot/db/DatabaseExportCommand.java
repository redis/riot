package com.redislabs.riot.db;

import com.redislabs.riot.AbstractExportCommand;
import com.redislabs.riot.processor.DataStructureMapItemProcessor;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;

import javax.sql.DataSource;
import java.util.Map;

@Command(name = "export", aliases = "e", description = "Export to a database")
public class DatabaseExportCommand extends AbstractExportCommand<Map<String, Object>> {

    @Mixin
    private DatabaseExportOptions options = new DatabaseExportOptions();

    @Override
    protected Flow flow() throws Exception {
        DataSource dataSource = options.dataSource();
        JdbcBatchItemWriterBuilder<Map<String, Object>> builder = new JdbcBatchItemWriterBuilder<>();
        builder.itemSqlParameterSourceProvider(MapSqlParameterSource::new);
        builder.dataSource(dataSource);
        builder.sql(options.getSql());
        builder.assertUpdates(!options.isNoAssertUpdates());
        JdbcBatchItemWriter<Map<String, Object>> writer = builder.build();
        writer.afterPropertiesSet();
        DataStructureMapItemProcessor processor = DataStructureMapItemProcessor.builder().keyRegex(options.getKeyRegex()).build();
        return flow(step(processor, writer).build());
    }

}
