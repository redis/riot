package com.redislabs.riot.db;

import java.util.Map;

import javax.sql.DataSource;

import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;

import com.redislabs.riot.AbstractExportCommand;
import com.redislabs.riot.processor.DataStructureMapItemProcessor;

import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

@Command(name = "export", aliases = "e", description = "Export to a database")
public class DatabaseExportCommand extends AbstractExportCommand<Map<String, Object>> {

	@Parameters(arity = "1", description = "SQL INSERT statement", paramLabel = "SQL")
	private String sql;
	@Option(names = "--key-regex", description = "Regex for key-field extraction (default: ${DEFAULT-VALUE})", paramLabel = "<str>")
	private String keyRegex = "\\w+:(?<id>.+)";
	@Mixin
	private DatabaseOptions options = new DatabaseOptions();
	@Option(names = "--no-assert-updates", description = "Disable insert verification")
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
	protected DataStructureMapItemProcessor processor() {
		return DataStructureMapItemProcessor.builder().keyRegex(keyRegex).build();
	}

}
