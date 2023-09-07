package com.redis.riot.cli;

import java.util.function.Supplier;

import com.redis.riot.core.AbstractKeyValueImport;
import com.redis.riot.core.StepBuilder;

import picocli.CommandLine.ArgGroup;

public abstract class AbstractKeyValueImportCommand extends AbstractJobCommand {

    @ArgGroup(exclusive = false, heading = "Redis writer options%n")
    RedisWriterArgs writerArgs = new RedisWriterArgs();

    @Override
    protected AbstractKeyValueImport getJobExecutable() {
        AbstractKeyValueImport executable = getKeyValueImportExecutable();
        executable.setRedisWriterOptions(writerArgs.writerOptions());
        return executable;
    }

    protected abstract AbstractKeyValueImport getKeyValueImportExecutable();

    @Override
    protected Supplier<String> extraMessage(StepBuilder<?, ?> step) {
        return null;
    }

}
