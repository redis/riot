package com.redis.riot.cli;

import java.util.function.Supplier;

import com.redis.riot.core.AbstractStructImport;
import com.redis.riot.core.StepBuilder;

import picocli.CommandLine.ArgGroup;

public abstract class AbstractStructImportCommand extends AbstractJobCommand {

    @ArgGroup(exclusive = false, heading = "Redis writer options%n")
    RedisOperationArgs writerArgs = new RedisOperationArgs();

    @Override
    protected AbstractStructImport getJobExecutable() {
        AbstractStructImport executable = getKeyValueImportExecutable();
        executable.setWriterOptions(writerArgs.writerOptions());
        return executable;
    }

    protected abstract AbstractStructImport getKeyValueImportExecutable();

    @Override
    protected Supplier<String> extraMessage(StepBuilder<?, ?> step) {
        return null;
    }

}
