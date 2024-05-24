package com.redis.riot;

import java.util.Map;

import org.springframework.batch.core.Job;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.item.database.builder.JdbcCursorItemReaderBuilder;
import org.springframework.util.Assert;

import com.redis.riot.core.Step;
import com.redis.riot.db.DatabaseReaderArgs;

import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Parameters;

@Command(name = "db-import", description = "Import from a relational database.")
public class DatabaseImport extends AbstractImport {

	@Parameters(arity = "1", description = "SQL SELECT statement", paramLabel = "SQL")
	private String sql;

	@ArgGroup(exclusive = false)
	private DatabaseReaderArgs databaseReaderArgs = new DatabaseReaderArgs();

	@ArgGroup(exclusive = false, heading = "Processor options%n")
	private ImportProcessorArgs processorArgs = new ImportProcessorArgs();

	public void copyTo(DatabaseImport target) {
		super.copyTo(target);
		target.databaseReaderArgs = databaseReaderArgs;
		target.processorArgs = processorArgs;
	}

	@Override
	protected Job job() {
		return job(new Step<>(reader(), mapWriter()).processor(mapProcessor(processorArgs)).taskName("Importing"));
	}

	private JdbcCursorItemReader<Map<String, Object>> reader() {
		Assert.notNull(sql, "No SQL statement specified");
		JdbcCursorItemReaderBuilder<Map<String, Object>> reader = databaseReaderArgs.reader();
		reader.name(sql);
		reader.sql(sql);
		return reader.build();
	}

	public String getSql() {
		return sql;
	}

	public void setSql(String sql) {
		this.sql = sql;
	}

	public DatabaseReaderArgs getDatabaseReaderArgs() {
		return databaseReaderArgs;
	}

	public void setDatabaseReaderArgs(DatabaseReaderArgs databaseReaderArgs) {
		this.databaseReaderArgs = databaseReaderArgs;
	}

}
