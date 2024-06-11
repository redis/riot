package com.redis.riot;

import org.springframework.batch.core.Job;

import com.redis.riot.db.DatabaseReaderArgs;

import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;

@Command(name = "db-import", description = "Import from a relational database.")
public class DatabaseImport extends AbstractImportCommand {

	@ArgGroup(exclusive = false)
	private DatabaseReaderArgs databaseReaderArgs = new DatabaseReaderArgs();

	@Override
	protected Job job() {
		return job(step(databaseReaderArgs.reader()).processor(mapProcessor()));
	}

	public DatabaseReaderArgs getDatabaseReaderArgs() {
		return databaseReaderArgs;
	}

	public void setDatabaseReaderArgs(DatabaseReaderArgs databaseReaderArgs) {
		this.databaseReaderArgs = databaseReaderArgs;
	}

}
