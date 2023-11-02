package com.redis.riot.cli;

import com.redis.riot.core.AbstractStructImport;

import picocli.CommandLine.ArgGroup;

public abstract class AbstractStructImportCommand extends AbstractJobCommand {

    @ArgGroup(exclusive = false, heading = "Writer options%n")
    RedisWriterArgs writerArgs = new RedisWriterArgs();

    @Override
    protected AbstractStructImport getJobExecutable() {
        AbstractStructImport executable = getKeyValueImportExecutable();
        executable.setWriterOptions(writerArgs.writerOptions());
        return executable;
    }

    protected abstract AbstractStructImport getKeyValueImportExecutable();

}
