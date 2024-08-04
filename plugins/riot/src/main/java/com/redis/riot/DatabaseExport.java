package com.redis.riot;

import org.springframework.batch.core.Job;

import com.redis.riot.db.DatabaseWriterArgs;

import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;

@Command(name = "db-export", description = "Export Redis data to a relational database.")
public class DatabaseExport extends AbstractExportCommand<RedisExecutionContext> {

	@ArgGroup(exclusive = false)
	private DatabaseWriterArgs databaseWriterArgs = new DatabaseWriterArgs();

	@Override
	protected RedisExecutionContext newExecutionContext() {
		return new RedisExecutionContext();
	}

	@Override
	protected Job job(RedisExecutionContext context) throws Exception {
		return job(context, step(context, databaseWriterArgs.writer()).processor(mapProcessor()));
	}

	public DatabaseWriterArgs getDatabaseWriterArgs() {
		return databaseWriterArgs;
	}

	public void setDatabaseWriterArgs(DatabaseWriterArgs args) {
		this.databaseWriterArgs = args;
	}

}
