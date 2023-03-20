package com.redis.riot.db;

import java.sql.Connection;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.sql.DataSource;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.job.builder.JobBuilderException;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.item.database.builder.JdbcCursorItemReaderBuilder;
import org.springframework.jdbc.core.ColumnMapRowMapper;

import com.redis.riot.AbstractImportCommand;
import com.redis.riot.JobCommandContext;
import com.redis.riot.ProgressMonitor;

import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Parameters;

@Command(name = "db-import", description = "Import data from a relational database")
public class DatabaseImportCommand extends AbstractImportCommand {

	private static final String COMMAND_NAME = "db-export";
	private static final Logger log = Logger.getLogger(DatabaseImportCommand.class.getName());

	@Parameters(arity = "1", description = "SQL SELECT statement", paramLabel = "SQL")
	private String sql;
	@Mixin
	private DataSourceOptions dataSourceOptions = new DataSourceOptions();
	@Mixin
	private DatabaseImportOptions importOptions = new DatabaseImportOptions();

	public DataSourceOptions getDataSourceOptions() {
		return dataSourceOptions;
	}

	public void setDataSourceOptions(DataSourceOptions dataSourceOptions) {
		this.dataSourceOptions = dataSourceOptions;
	}

	public DatabaseImportOptions getImportOptions() {
		return importOptions;
	}

	public void setImportOptions(DatabaseImportOptions importOptions) {
		this.importOptions = importOptions;
	}

	@Override
	protected Job job(JobCommandContext context) {
		log.log(Level.FINE, "Creating data source: {0}", dataSourceOptions);
		DataSource dataSource = dataSourceOptions.dataSource();
		try (Connection connection = dataSource.getConnection()) {
			String productName = connection.getMetaData().getDatabaseProductName();
			log.log(Level.FINE, "Creating {0} database reader: {1}", new Object[] { productName, importOptions });
			JdbcCursorItemReaderBuilder<Map<String, Object>> builder = new JdbcCursorItemReaderBuilder<>();
			builder.saveState(false);
			builder.dataSource(dataSource);
			builder.name(productName + "-database-reader");
			builder.rowMapper(new ColumnMapRowMapper());
			builder.sql(sql);
			importOptions.configure(builder);
			JdbcCursorItemReader<Map<String, Object>> reader = builder.build();
			reader.afterPropertiesSet();
			ProgressMonitor monitor = progressMonitor().task("Importing from " + productName).build();
			return context.job(COMMAND_NAME).start(step(step(context, COMMAND_NAME, reader), monitor).build()).build();
		} catch (Exception e) {
			throw new JobBuilderException(e);
		}
	}

}
