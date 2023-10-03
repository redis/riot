package com.redis.riot.cli;

import java.util.function.Supplier;

import com.redis.riot.core.AbstractExport;
import com.redis.riot.core.AbstractJobRunnable;
import com.redis.riot.core.StepBuilder;
import com.redis.spring.batch.reader.ScanSizeEstimator;
import com.redis.spring.batch.reader.StructItemReader;

import picocli.CommandLine.ArgGroup;

public abstract class AbstractExportCommand extends AbstractJobCommand {

    @ArgGroup(exclusive = false, heading = "Redis reader options%n")
    RedisReaderArgs readerArgs = new RedisReaderArgs();

    @ArgGroup(exclusive = false)
    KeyFilterArgs keyFilterArgs = new KeyFilterArgs();

    @ArgGroup(exclusive = false, heading = "Processor options%n")
    KeyValueProcessorArgs processorArgs = new KeyValueProcessorArgs();

    @Override
    protected AbstractJobRunnable getJobExecutable() {
        AbstractExport export = getExport();
        export.setReaderOptions(readerArgs.readerOptions());
        export.setKeyFilterOptions(keyFilterArgs.keyFilterOptions());
        export.setEvaluationContextOptions(evaluationContextOptions());
        export.setProcessorOptions(processorArgs.processorOptions());
        return export;
    }

    protected abstract AbstractExport getExport();

    @SuppressWarnings("unchecked")
    @Override
    protected long size(StepBuilder<?, ?> step) {
        StructItemReader<String, String> reader = (StructItemReader<String, String>) step.getReader();
        ScanSizeEstimator estimator = new ScanSizeEstimator(reader.getClient());
        estimator.setScanMatch(readerArgs.scanMatch);
        if (readerArgs.scanType != null) {
            estimator.setScanType(readerArgs.scanType.getString());
        }
        return estimator.getAsLong();
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
