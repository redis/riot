package com.redis.riot;

import java.util.Map;

import javax.sql.DataSource;

import org.springframework.batch.core.Job;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.item.database.builder.JdbcCursorItemReaderBuilder;
import org.springframework.jdbc.core.ColumnMapRowMapper;
import org.springframework.util.Assert;

import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Parameters;

@Command(name = "db-import", description = "Import from a relational database.")
public class DatabaseImport extends AbstractImportCommand {

	@ArgGroup(exclusive = false)
	private DataSourceArgs dataSourceArgs = new DataSourceArgs();

	@Parameters(arity = "1", description = "SQL SELECT statement", paramLabel = "SQL")
	private String sql;

	@ArgGroup(exclusive = false)
	private DatabaseReaderArgs readerArgs = new DatabaseReaderArgs();

	@Override
	protected Job job() {
		return job(step(reader()));
	}

	private JdbcCursorItemReader<Map<String, Object>> reader() {
		Assert.hasLength(sql, "No SQL statement specified");
		log.info("Creating data source with {}", dataSourceArgs);
		DataSource dataSource = dataSourceArgs.dataSource();
		log.info("Creating JDBC reader with sql=\"{}\" {}", sql, readerArgs);
		JdbcCursorItemReaderBuilder<Map<String, Object>> reader = new JdbcCursorItemReaderBuilder<>();
		reader.dataSource(dataSource);
		reader.sql(sql);
		reader.saveState(false);
		reader.rowMapper(new ColumnMapRowMapper());
		reader.fetchSize(readerArgs.getFetchSize());
		reader.maxRows(readerArgs.getMaxRows());
		reader.queryTimeout(readerArgs.getQueryTimeout());
		reader.useSharedExtendedConnection(readerArgs.isUseSharedExtendedConnection());
		reader.verifyCursorPosition(readerArgs.isVerifyCursorPosition());
		if (readerArgs.getMaxItemCount() > 0) {
			reader.maxItemCount(readerArgs.getMaxItemCount());
		}
		reader.name(sql);
		return reader.build();
	}

	public String getSql() {
		return sql;
	}

	public void setSql(String sql) {
		this.sql = sql;
	}

	public DatabaseReaderArgs getReaderArgs() {
		return readerArgs;
	}

	public void setReaderArgs(DatabaseReaderArgs args) {
		this.readerArgs = args;
	}

	public DataSourceArgs getDataSourceArgs() {
		return dataSourceArgs;
	}

	public void setDataSourceArgs(DataSourceArgs args) {
		this.dataSourceArgs = args;
	}

}
