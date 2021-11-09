package com.redis.riot.db;

import java.sql.Connection;
import java.util.Map;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;

import com.redis.riot.AbstractExportCommand;
import com.redis.riot.processor.DataStructureItemProcessor;

import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;

@Command(name = "export", description = "Export to a database")
public class DatabaseExportCommand extends AbstractExportCommand<Map<String, Object>> {

	private static final Logger log = LoggerFactory.getLogger(DatabaseExportCommand.class);

	private static final String NAME = "db-export";

	@CommandLine.Parameters(arity = "1", description = "SQL INSERT statement.", paramLabel = "SQL")
	private String sql;
	@Mixin
	private DataSourceOptions dataSourceOptions = new DataSourceOptions();
	@Mixin
	private DatabaseExportOptions exportOptions = new DatabaseExportOptions();

	public DatabaseExportOptions getExportOptions() {
		return exportOptions;
	}

	public DataSourceOptions getDataSourceOptions() {
		return dataSourceOptions;
	}

	@Override
	protected Flow flow() throws Exception {
		log.info("Creating data source: {}", dataSourceOptions);
		DataSource dataSource = dataSourceOptions.dataSource();
		try (Connection connection = dataSource.getConnection()) {
			String dbName = connection.getMetaData().getDatabaseProductName();
			log.info("Creating {} database writer: {}", dbName, exportOptions);
			JdbcBatchItemWriterBuilder<Map<String, Object>> builder = new JdbcBatchItemWriterBuilder<>();
			builder.itemSqlParameterSourceProvider(NullableMapSqlParameterSource::new);
			builder.dataSource(dataSource);
			builder.sql(sql);
			builder.assertUpdates(exportOptions.isAssertUpdates());
			JdbcBatchItemWriter<Map<String, Object>> writer = builder.build();
			writer.afterPropertiesSet();
			DataStructureItemProcessor processor = DataStructureItemProcessor.of(exportOptions.getKeyRegex());
			return flow(NAME, step(NAME, String.format("Exporting to %s", dbName), processor, writer).build());
		}
	}

}
