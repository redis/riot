package com.redis.riot.db;

import java.sql.Connection;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.sql.DataSource;

import org.springframework.batch.core.Job;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.item.database.builder.JdbcCursorItemReaderBuilder;
import org.springframework.jdbc.core.ColumnMapRowMapper;

import com.redis.riot.AbstractImportCommand;
import com.redis.riot.JobCommandContext;
import com.redis.riot.ProgressMonitor;

import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Parameters;

@Command(name = "import", description = "Import from a database")
public class DatabaseImportCommand extends AbstractImportCommand {

	private static final Logger log = Logger.getLogger(DatabaseImportCommand.class.getName());

	private static final String NAME = "db-import";

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
	protected Job job(JobCommandContext context) throws Exception {
		log.log(Level.FINE, "Creating data source: {0}", dataSourceOptions);
		DataSource dataSource = dataSourceOptions.dataSource();
		try (Connection connection = dataSource.getConnection()) {
			String name = connection.getMetaData().getDatabaseProductName();
			log.log(Level.FINE, "Creating {0} database reader: {1}", new Object[] { name, importOptions });
			JdbcCursorItemReaderBuilder<Map<String, Object>> builder = new JdbcCursorItemReaderBuilder<>();
			builder.saveState(false);
			builder.dataSource(dataSource);
			builder.name(name + "-database-reader");
			builder.rowMapper(new ColumnMapRowMapper());
			builder.sql(sql);
			importOptions.configure(builder);
			JdbcCursorItemReader<Map<String, Object>> reader = builder.build();
			reader.afterPropertiesSet();
			ProgressMonitor monitor = progressMonitor().task("Importing from " + name).build();
			return job(context, NAME, reader, monitor);
		}
	}

}
