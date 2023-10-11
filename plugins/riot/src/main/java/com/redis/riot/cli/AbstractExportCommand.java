package com.redis.riot.cli;

import java.util.function.LongSupplier;

import com.redis.riot.core.AbstractExport;
import com.redis.riot.core.AbstractJobRunnable;
import com.redis.riot.core.RiotStep;
import com.redis.spring.batch.RedisItemReader;
import com.redis.spring.batch.reader.ScanSizeEstimator;

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

    @Override
    protected LongSupplier initialMaxSupplier(RiotStep<?, ?> step) {
        RedisItemReader<?, ?, ?> reader = (RedisItemReader<?, ?, ?>) step.getReader();
        if (reader.isLive()) {
            return () -> ProgressStepExecutionListener.UNKNOWN_SIZE;
        }
        ScanSizeEstimator estimator = new ScanSizeEstimator(reader.getClient());
        estimator.setScanMatch(readerArgs.scanMatch);
        if (readerArgs.scanType != null) {
            estimator.setScanType(readerArgs.scanType.getString());
        }
        return estimator;
    }

}
