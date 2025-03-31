package com.redis.riot;

import java.util.Map;

import org.springframework.batch.core.Job;
import org.springframework.batch.item.database.builder.JdbcCursorItemReaderBuilder;

import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Parameters;

@Command(name = "db-import", description = "Import from a relational database.")
public class DatabaseImport extends AbstractRedisImportCommand {

	@ArgGroup(exclusive = false)
	private DataSourceArgs dataSourceArgs = new DataSourceArgs();

	@Parameters(arity = "1", description = "SQL SELECT statement", paramLabel = "SQL")
	protected String sql;

	@ArgGroup(exclusive = false)
	private DatabaseReaderArgs readerArgs = new DatabaseReaderArgs();

	@Override
	protected Job job() {
		log.info("Creating JDBC reader with sql=\"{}\" {} {}", sql, dataSourceArgs, readerArgs);
		JdbcCursorItemReaderBuilder<Map<String, Object>> reader = JdbcCursorItemReaderFactory.create(sql,
				dataSourceArgs, readerArgs);
		return job(step(reader.build()));
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
