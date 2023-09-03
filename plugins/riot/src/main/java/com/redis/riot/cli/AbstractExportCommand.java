package com.redis.riot.cli;

import com.redis.riot.core.AbstractExport;
import com.redis.riot.core.AbstractJobExecutable;
import com.redis.riot.core.StepBuilder;
import com.redis.spring.batch.util.BatchUtils;

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

    @Override
    protected long size(StepBuilder<?, ?> step) {
        return BatchUtils.size(step.getReader());
    }

    @Override
    protected String taskName(StepBuilder<?, ?> step) {
        return "Exporting";
    }

}
