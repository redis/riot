package com.redis.riot.cli;

import com.redis.riot.core.AbstractExport;
import com.redis.riot.core.AbstractJobExecutable;

import picocli.CommandLine.ArgGroup;

public abstract class AbstractExportCommand extends AbstractJobCommand {

    @ArgGroup(exclusive = false, heading = "Redis reader options%n")
    private RedisReaderArgs redisReaderArgs = new RedisReaderArgs();

    @Override
    protected AbstractJobExecutable getJobExecutable() {
        AbstractExport executable = getExportExecutable();
        executable.setRedisReaderOptions(redisReaderArgs.redisReaderOptions());
        return executable;
    }

    protected abstract AbstractExport getExportExecutable();

}
