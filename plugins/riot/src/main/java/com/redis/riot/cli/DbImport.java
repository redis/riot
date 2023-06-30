package com.redis.riot.cli;

import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.sql.DataSource;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.job.builder.JobBuilderException;
import org.springframework.batch.core.step.builder.SimpleStepBuilder;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.item.database.builder.JdbcCursorItemReaderBuilder;
import org.springframework.jdbc.core.ColumnMapRowMapper;

import com.redis.riot.cli.common.AbstractImportCommand;
import com.redis.riot.cli.common.CommandContext;
import com.redis.riot.cli.common.DatabaseHelper;
import com.redis.riot.cli.common.StepProgressMonitor;
import com.redis.riot.cli.db.DbImportOptions;

import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Parameters;

@Command(name = "db-import", description = "Import from a relational database.")
public class DbImport extends AbstractImportCommand {

	private static final Logger log = Logger.getLogger(DbImport.class.getName());
	private static final String TASK_NAME = "Importing from database";

	@Parameters(arity = "1", description = "SQL SELECT statement", paramLabel = "SQL")
	private String sql;
	@Mixin
	private DbImportOptions options = new DbImportOptions();

	public DbImportOptions getOptions() {
		return options;
	}

	public void setOptions(DbImportOptions options) {
		this.options = options;
	}

	@Override
	protected Job job(CommandContext context) {
		log.log(Level.FINE, "Creating data source: {0}", options.getDataSourceOptions());
		DataSource dataSource = DatabaseHelper.dataSource(options.getDataSourceOptions());
		log.log(Level.FINE, "Creating database reader with {0}", options);
		JdbcCursorItemReaderBuilder<Map<String, Object>> builder = new JdbcCursorItemReaderBuilder<>();
		builder.saveState(false);
		builder.dataSource(dataSource);
		builder.name("database-reader");
		builder.rowMapper(new ColumnMapRowMapper());
		builder.sql(sql);
		configure(builder);
		JdbcCursorItemReader<Map<String, Object>> reader = builder.build();
		try {
			reader.afterPropertiesSet();
		} catch (Exception e) {
			throw new JobBuilderException(e);
		}
		SimpleStepBuilder<Map<String, Object>, Map<String, Object>> step = step(context.getRedisClient(), reader);
		StepProgressMonitor monitor = monitor(TASK_NAME);
		monitor.register(step);
		return job(commandName()).start(step.build()).build();
	}

	public void configure(JdbcCursorItemReaderBuilder<Map<String, Object>> builder) {
		builder.fetchSize(options.getFetchSize());
		builder.maxRows(options.getMaxResultSetRows());
		builder.queryTimeout(options.getQueryTimeout());
		builder.useSharedExtendedConnection(options.isUseSharedExtendedConnection());
		builder.verifyCursorPosition(options.isVerifyCursorPosition());
		if (options.getMaxItemCount() > 0) {
			builder.maxItemCount(options.getMaxItemCount());
		}
	}

}
