package com.redis.riot.cli;

import java.util.concurrent.Callable;

import org.springframework.batch.item.ItemReader;

import com.redis.riot.core.AbstractExport;
import com.redis.riot.core.AbstractJobRunnable;
import com.redis.spring.batch.RedisItemReader;
import com.redis.spring.batch.reader.ScanSizeEstimator;

import picocli.CommandLine.ArgGroup;

public abstract class AbstractExportCommand extends AbstractJobCommand {

	@ArgGroup(exclusive = false, heading = "Reader options%n")
	RedisReaderArgs readerArgs = new RedisReaderArgs();

	@ArgGroup(exclusive = false, heading = "Processor options%n")
	KeyValueProcessorArgs processorArgs = new KeyValueProcessorArgs();

	@Override
	protected AbstractJobRunnable jobRunnable() {
		AbstractExport export = exportRunnable();
		export.setReaderOptions(readerArgs.readerOptions());
		export.setProcessorOptions(processorArgs.processorOptions());
		return export;
	}

	protected abstract AbstractExport exportRunnable();

	@Override
	protected Callable<Long> initialMaxSupplier(String stepName, ItemReader<?> reader) {
		if (((RedisItemReader<?, ?, ?>) reader).isLive()) {
			return () -> ProgressStepExecutionListener.UNKNOWN_SIZE;
		}
		ScanSizeEstimator estimator = new ScanSizeEstimator(((RedisItemReader<?, ?, ?>) reader).getClient());
		estimator.setScanMatch(readerArgs.scanMatch);
		if (readerArgs.scanType != null) {
			estimator.setScanType(readerArgs.scanType.getString());
		}
		return estimator;
	}

}
