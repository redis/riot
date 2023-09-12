package com.redis.riot.cli;

import java.util.function.Supplier;

import com.redis.riot.core.AbstractExport;
import com.redis.riot.core.AbstractJobExecutable;
import com.redis.riot.core.RedisReaderOptions;
import com.redis.riot.core.StepBuilder;
import com.redis.spring.batch.util.BatchUtils;

import picocli.CommandLine.ArgGroup;

public abstract class AbstractExportCommand<K, V> extends AbstractJobCommand {

    @ArgGroup(exclusive = false, heading = "Redis reader options%n")
    RedisReaderArgs readerArgs = new RedisReaderArgs();

    @ArgGroup(exclusive = false, heading = "Processor options%n")
    KeyValueProcessorArgs processorArgs = new KeyValueProcessorArgs();

    @Override
    protected AbstractJobExecutable getJobExecutable() {
        AbstractExport<K, V> executable = getExportExecutable();
        executable.setReaderOptions(readerOptions());
        executable.setEvaluationContextOptions(evaluationContextOptions());
        executable.setProcessorOptions(processorArgs.processorOptions());
        return executable;
    }

    protected RedisReaderOptions readerOptions() {
        RedisReaderOptions options = readerArgs.redisReaderOptions();
        options.setDatabase(parent.redisArgs.redisClientOptions().getUriOptions().redisURI().getDatabase());
        return options;
    }

    protected abstract AbstractExport<K, V> getExportExecutable();

    @Override
    protected long size(StepBuilder<?, ?> step) {
        return BatchUtils.size(step.getReader());
    }

    @Override
    protected String taskName(StepBuilder<?, ?> step) {
        return "Exporting";
    }

    @Override
    protected Supplier<String> extraMessage(StepBuilder<?, ?> step) {
        return null;
    }

}
