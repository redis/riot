package com.redis.riot.cli;

import com.redis.riot.core.AbstractMapImport;
import com.redis.riot.core.StepBuilder;
import com.redis.riot.db.DatabaseImport;
import com.redis.spring.batch.util.BatchUtils;

import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Parameters;

@Command(name = "db-import", description = "Import from a relational database.")
public class DatabaseImportCommand extends AbstractImportCommand {

    @Parameters(arity = "1", description = "SQL SELECT statement", paramLabel = "SQL")
    String sql;

    @ArgGroup(exclusive = false)
    DatabaseImportArgs args = new DatabaseImportArgs();

    @Override
    protected AbstractMapImport getMapImportExecutable() {
        DatabaseImport executable = new DatabaseImport(redisClient(), sql);
        executable.setDataSourceOptions(args.dataSourceOptions());
        executable.setFetchSize(args.fetchSize);
        executable.setMaxItemCount(args.maxItemCount);
        executable.setMaxResultSetRows(args.maxResultSetRows);
        return executable;
    }

    @Override
    protected long size(StepBuilder<?, ?> step) {
        return BatchUtils.SIZE_UNKNOWN;
    }

}
