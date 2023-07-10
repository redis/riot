package com.redis.riot.cli;

import java.util.Map;

import javax.sql.DataSource;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.job.builder.JobBuilderException;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.item.database.builder.JdbcCursorItemReaderBuilder;
import org.springframework.jdbc.core.ColumnMapRowMapper;

import com.redis.riot.cli.common.AbstractOperationImportCommand;
import com.redis.riot.cli.common.CommandContext;
import com.redis.riot.cli.common.DatabaseHelper;
import com.redis.riot.cli.common.DbImportOptions;

import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Parameters;

@Command(name = "db-import", description = "Import from a relational database.")
public class DbImport extends AbstractOperationImportCommand {

	private static final String TASK = "Importing from database";

	@Parameters(arity = "1", description = "SQL SELECT statement", paramLabel = "SQL")
	private String sql;
	@Mixin
	private DbImportOptions dbImportOptions = new DbImportOptions();

	public DbImportOptions getDbImportOptions() {
		return dbImportOptions;
	}

	public void setDbImportOptions(DbImportOptions options) {
		this.dbImportOptions = options;
	}

	@Override
	protected Job job(CommandContext context) {
		return job(step(context, reader()).task(TASK));
	}

	private ItemReader<Map<String, Object>> reader() {
		DataSource dataSource = DatabaseHelper.dataSource(dbImportOptions.getDataSourceOptions());
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
		return reader;
	}

	public void configure(JdbcCursorItemReaderBuilder<Map<String, Object>> builder) {
		builder.fetchSize(dbImportOptions.getFetchSize());
		builder.maxRows(dbImportOptions.getMaxResultSetRows());
		builder.queryTimeout(dbImportOptions.getQueryTimeout());
		builder.useSharedExtendedConnection(dbImportOptions.isUseSharedExtendedConnection());
		builder.verifyCursorPosition(dbImportOptions.isVerifyCursorPosition());
		if (dbImportOptions.getMaxItemCount() > 0) {
			builder.maxItemCount(dbImportOptions.getMaxItemCount());
		}
	}

	@Override
	public String toString() {
		return "DbImport [sql=" + sql + ", dbImportOptions=" + dbImportOptions + ", processorOptions=" + processorOptions
				+ ", writerOptions=" + writerOptions + ", jobOptions=" + jobOptions + "]";
	}

}
