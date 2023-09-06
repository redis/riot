package com.redis.riot.cli;

import com.redis.riot.db.DatabaseExport;

import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Parameters;

@Command(name = "db-export", description = "Export Redis data to a relational database.")
public class DatabaseExportCommand extends AbstractExportCommand {

    @Parameters(arity = "1", description = "SQL INSERT statement.", paramLabel = "SQL")
    String sql;

    @ArgGroup(exclusive = false)
    DatabaseExportArgs databaseExportArgs = new DatabaseExportArgs();

    @Override
    protected DatabaseExport getExportExecutable() {
        DatabaseExport executable = new DatabaseExport(redisClient(), sql);
        executable.setAssertUpdates(databaseExportArgs.assertUpdates);
        executable.setDataSourceOptions(databaseExportArgs.dataSourceOptions());
        executable.setKeyPattern(databaseExportArgs.keyRegex);
        return executable;
    }

}
