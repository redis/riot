package com.redis.riot;

import org.springframework.batch.core.Job;

import com.redis.riot.db.DatabaseReaderArgs;

import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;

@Command(name = "db-import", description = "Import from a relational database.")
public class DatabaseImport extends AbstractImportCommand<RedisExecutionContext> {

	@ArgGroup(exclusive = false)
	private DatabaseReaderArgs databaseReaderArgs = new DatabaseReaderArgs();

	@Override
	protected RedisExecutionContext newExecutionContext() {
		return new RedisExecutionContext();
	}

	@Override
	protected Job job(RedisExecutionContext context) {
		return job(context, step(context, databaseReaderArgs.reader()));
	}

	public DatabaseReaderArgs getDatabaseReaderArgs() {
		return databaseReaderArgs;
	}

	public void setDatabaseReaderArgs(DatabaseReaderArgs databaseReaderArgs) {
		this.databaseReaderArgs = databaseReaderArgs;
	}

}
