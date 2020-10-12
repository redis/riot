package com.redislabs.riot.db;

import java.util.Map;

import javax.sql.DataSource;

import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;

import com.redislabs.riot.AbstractExportCommand;
import com.redislabs.riot.processor.KeyValueMapItemProcessor;

import picocli.CommandLine;

@CommandLine.Command(name = "export", aliases = "e", description = "Export to a database")
public class DatabaseExportCommand extends AbstractExportCommand<Map<String, Object>> {

	@CommandLine.Parameters(arity = "1", description = "SQL INSERT statement", paramLabel = "SQL")
	private String sql;
	@CommandLine.Mixin
	private DatabaseOptions options = new DatabaseOptions();
	@CommandLine.Option(names = "--no-assert-updates", description = "Disable insert verification")
	private boolean noAssertUpdates;

	@Override
	protected JdbcBatchItemWriter<Map<String, Object>> writer() throws Exception {
		DataSource dataSource = options.dataSource();
		JdbcBatchItemWriterBuilder<Map<String, Object>> builder = new JdbcBatchItemWriterBuilder<>();
		builder.itemSqlParameterSourceProvider(MapSqlParameterSource::new);
		builder.dataSource(dataSource);
		builder.sql(sql);
		builder.assertUpdates(!noAssertUpdates);
		JdbcBatchItemWriter<Map<String, Object>> writer = builder.build();
		writer.afterPropertiesSet();
		return writer;
	}

	@Override
	protected KeyValueMapItemProcessor processor() {
		return keyValueMapItemProcessor();
	}

}
