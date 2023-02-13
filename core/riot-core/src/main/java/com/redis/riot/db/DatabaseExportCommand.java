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
import com.redis.riot.processor.DataStructureToMapProcessor;
import com.redis.spring.batch.common.DataStructure;

import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Parameters;

@Command(name = "db-export", description = "Export Redis data to a relational database")
public class DatabaseExportCommand extends AbstractExportCommand {

	private static Logger log = Logger.getLogger(DatabaseExportCommand.class.getName());

	@Parameters(arity = "1", description = "SQL INSERT statement.", paramLabel = "SQL")
	private String sql;
	@Mixin
	private DataSourceOptions dataSourceOptions = new DataSourceOptions();
	@Mixin
	private DatabaseExportOptions options = new DatabaseExportOptions();

	public DatabaseExportOptions getOptions() {
		return options;
	}

	public void setOptions(DatabaseExportOptions exportOptions) {
		this.options = exportOptions;
	}

	public DataSourceOptions getDataSourceOptions() {
		return dataSourceOptions;
	}

	public void setDataSourceOptions(DataSourceOptions dataSourceOptions) {
		this.dataSourceOptions = dataSourceOptions;
	}

	public String getSql() {
		return sql;
	}

	public void setSql(String sql) {
		this.sql = sql;
	}

	@Override
	protected Job job(JobCommandContext context) throws SQLException {
		log.log(Level.FINE, "Creating data source with {0}", dataSourceOptions);
		DataSource dataSource = dataSourceOptions.dataSource();
		try (Connection connection = dataSource.getConnection()) {
			String dbName = connection.getMetaData().getDatabaseProductName();
			log.log(Level.FINE, "Creating writer for database {0} with {1}", new Object[] { dbName, options });
			JdbcBatchItemWriterBuilder<Map<String, Object>> builder = new JdbcBatchItemWriterBuilder<>();
			builder.itemSqlParameterSourceProvider(NullableMapSqlParameterSource::new);
			builder.dataSource(dataSource);
			builder.sql(sql);
			builder.assertUpdates(options.isAssertUpdates());
			JdbcBatchItemWriter<Map<String, Object>> writer = builder.build();
			writer.afterPropertiesSet();
			ItemProcessor<DataStructure<String>, Map<String, Object>> processor = DataStructureToMapProcessor
					.of(options.getKeyRegex());
			String task = String.format("Exporting to %s", dbName);
			return job(context, commandSpec.name(),
					step(context, commandSpec.name(), reader(context), processor, writer), task);
		}
	}

}
