package com.redis.riot.cli;

import com.redis.riot.core.db.DatabaseExport;

import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

@Command(name = "db-export", description = "Export Redis data to a relational database.")
public class DatabaseExportCommand extends AbstractExportCommand {

    @Parameters(arity = "1", description = "SQL INSERT statement.", paramLabel = "SQL")
    private String sql;

    @ArgGroup(exclusive = false, heading = "JDBC driver options%n")
    private DataSourceArgs dataSourceArgs = new DataSourceArgs();

    @Option(names = "--key-regex", description = "Regex for key-field extraction (default: ${DEFAULT-VALUE}).", paramLabel = "<str>")
    private String keyRegex = DatabaseExport.DEFAULT_KEY_REGEX;

    @Option(names = "--no-assert-updates", description = "Confirm every insert results in update of at least one row. True by default.", negatable = true)
    private boolean assertUpdates = DatabaseExport.DEFAULT_ASSERT_UPDATES;

    @Override
    protected DatabaseExport getExportExecutable() {
        DatabaseExport executable = new DatabaseExport(redisClient(), sql);
        executable.setAssertUpdates(assertUpdates);
        executable.setDataSourceOptions(dataSourceArgs.dataSourceOptions());
        executable.setKeyRegex(keyRegex);
        return executable;
    }

}
