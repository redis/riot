package com.redis.riot.cli;

import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.sql.DataSource;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.job.builder.JobBuilderException;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.item.database.builder.JdbcCursorItemReaderBuilder;
import org.springframework.jdbc.core.ColumnMapRowMapper;

import com.redis.riot.cli.common.AbstractImportCommand;
import com.redis.riot.cli.common.CommandContext;
import com.redis.riot.cli.common.ProgressMonitor;
import com.redis.riot.cli.db.DataSourceOptions;
import com.redis.riot.cli.db.DbImportOptions;

import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Parameters;

@Command(name = "db-import", description = "Import from relational databases")
public class DbImport extends AbstractImportCommand {

	private static final Logger log = Logger.getLogger(DbImport.class.getName());
	private static final String TASK_NAME = "Importing from database";

	@Parameters(arity = "1", description = "SQL SELECT statement", paramLabel = "SQL")
	private String sql;
	@Mixin
	private DataSourceOptions dataSourceOptions = new DataSourceOptions();
	@Mixin
	private DbImportOptions importOptions = new DbImportOptions();

	public DataSourceOptions getDataSourceOptions() {
		return dataSourceOptions;
	}

	public void setDataSourceOptions(DataSourceOptions dataSourceOptions) {
		this.dataSourceOptions = dataSourceOptions;
	}

	public DbImportOptions getImportOptions() {
		return importOptions;
	}

	public void setImportOptions(DbImportOptions importOptions) {
		this.importOptions = importOptions;
	}

	@Override
	protected Job job(CommandContext context) {
		log.log(Level.FINE, "Creating data source: {0}", dataSourceOptions);
		DataSource dataSource = dataSourceOptions.dataSource();
		log.log(Level.FINE, "Creating database reader with {0}", importOptions);
		JdbcCursorItemReaderBuilder<Map<String, Object>> builder = new JdbcCursorItemReaderBuilder<>();
		builder.saveState(false);
		builder.dataSource(dataSource);
		builder.name("database-reader");
		builder.rowMapper(new ColumnMapRowMapper());
		builder.sql(sql);
		importOptions.configure(builder);
		JdbcCursorItemReader<Map<String, Object>> reader = builder.build();
		try {
			reader.afterPropertiesSet();
		} catch (Exception e) {
			throw new JobBuilderException(e);
		}
		ProgressMonitor monitor = progressMonitor().task(TASK_NAME).build();
		return context.getJobRunner().job(commandName()).start(step(step(context, reader), monitor).build()).build();
	}

}
