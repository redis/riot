package com.redislabs.riot.db;

import com.redislabs.riot.AbstractExportCommand;
import com.redislabs.riot.Transfer;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import picocli.CommandLine;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

@CommandLine.Command(name = "export", aliases = "e", description = "Export to a database")
public class DatabaseExportCommand extends AbstractExportCommand<Map<String, Object>> {

    @CommandLine.Parameters(arity = "1", description = "SQL INSERT statement", paramLabel = "SQL")
    private String sql;
    @CommandLine.Mixin
    private DatabaseOptions options = new DatabaseOptions();
    @CommandLine.Option(names = "--no-assert-updates", description = "Disable insert verification")
    private boolean noAssertUpdates;

    @Override
    @SuppressWarnings("unchecked")
    protected List<Transfer<Object, Map<String, Object>>> transfers() throws SQLException {
        DataSource dataSource = options.dataSource();
        JdbcBatchItemWriterBuilder<Map<String, Object>> builder = new JdbcBatchItemWriterBuilder<>();
        builder.itemSqlParameterSourceProvider(MapSqlParameterSource::new);
        builder.dataSource(dataSource);
        builder.sql(sql);
        builder.assertUpdates(!noAssertUpdates);
        JdbcBatchItemWriter<Map<String, Object>> writer = builder.build();
        writer.afterPropertiesSet();
        return transfers("Exporting to " + options.name(dataSource), reader(), mapProcessor(), writer);
    }

}
