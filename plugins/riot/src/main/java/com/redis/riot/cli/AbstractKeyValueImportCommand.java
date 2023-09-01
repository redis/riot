package com.redis.riot.cli;

import com.redis.riot.core.AbstractKeyValueImport;

import picocli.CommandLine.ArgGroup;

public abstract class AbstractKeyValueImportCommand extends AbstractJobCommand {

    @ArgGroup(exclusive = false, heading = "Redis writer options%n")
    private RedisWriterArgs writerArgs = new RedisWriterArgs();

    @Override
    protected AbstractKeyValueImport getJobExecutable() {
        AbstractKeyValueImport executable = getKeyValueImportExecutable();
        executable.setRedisWriterOptions(writerArgs.redisWriterOptions());
        return executable;
    }

    protected abstract AbstractKeyValueImport getKeyValueImportExecutable();

}
