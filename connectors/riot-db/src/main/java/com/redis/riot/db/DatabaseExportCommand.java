package com.redis.riot.db;

import java.sql.Connection;
import java.util.Map;
import java.util.Optional;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.sql.DataSource;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;

import com.redis.riot.AbstractExportCommand;
import com.redis.riot.processor.DataStructureItemProcessor;
import com.redis.spring.batch.DataStructure;

import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Parameters;

@Command(name = "export", description = "Export to a database")
public class DatabaseExportCommand extends AbstractExportCommand<Map<String, Object>> {

	private static Logger log = Logger.getLogger(DatabaseExportCommand.class.getName());

	private static final String NAME = "db-export";

	@Parameters(arity = "1", description = "SQL INSERT statement.", paramLabel = "SQL")
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
	protected Job job(JobBuilder jobBuilder) throws Exception {
		log.log(Level.FINE, "Creating data source with {0}", dataSourceOptions);
		DataSource dataSource = dataSourceOptions.dataSource();
		try (Connection connection = dataSource.getConnection()) {
			String dbName = connection.getMetaData().getDatabaseProductName();
			log.log(Level.FINE, "Creating writer for database {0} with {1}", new Object[] { dbName, exportOptions });
			JdbcBatchItemWriterBuilder<Map<String, Object>> builder = new JdbcBatchItemWriterBuilder<>();
			builder.itemSqlParameterSourceProvider(NullableMapSqlParameterSource::new);
			builder.dataSource(dataSource);
			builder.sql(sql);
			builder.assertUpdates(exportOptions.isAssertUpdates());
			JdbcBatchItemWriter<Map<String, Object>> writer = builder.build();
			writer.afterPropertiesSet();
			Optional<ItemProcessor<DataStructure<String>, Map<String, Object>>> processor = Optional
					.of(DataStructureItemProcessor.of(exportOptions.getKeyRegex()));
			return jobBuilder.start(step(NAME, String.format("Exporting to %s", dbName), processor, writer).build())
					.build();
		}
	}

}
