package com.redis.riot.db;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.sql.DataSource;

import org.springframework.batch.core.Job;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;

import com.redis.riot.AbstractExportCommand;
import com.redis.riot.JobCommandContext;
import com.redis.riot.processor.DataStructureItemProcessor;
import com.redis.spring.batch.DataStructure;

import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Parameters;

@Command(name = "export", description = "Export to a database")
public class DatabaseExportCommand extends AbstractExportCommand {

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
	protected Job job(JobCommandContext context) throws SQLException {
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
			ItemProcessor<DataStructure<String>, Map<String, Object>> processor = DataStructureItemProcessor
					.of(exportOptions.getKeyRegex());
			String task = String.format("Exporting to %s", dbName);
			return job(context, NAME, step(context, NAME, reader(context), processor, writer), task);
		}
	}

}
