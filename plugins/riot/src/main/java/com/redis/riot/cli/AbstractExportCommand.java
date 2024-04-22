package com.redis.riot.cli;

import java.util.function.LongSupplier;

import org.springframework.batch.item.ItemReader;

import com.redis.riot.core.AbstractExport;
import com.redis.riot.core.AbstractRedisRunnable;
import com.redis.spring.batch.RedisItemReader;
import com.redis.spring.batch.RedisItemReader.ReaderMode;
import com.redis.spring.batch.reader.ScanSizeEstimator;

import picocli.CommandLine.ArgGroup;

public abstract class AbstractExportCommand extends AbstractRiotCommand {

	@ArgGroup(exclusive = false, heading = "Reader options%n")
	RedisReaderArgs readerArgs = new RedisReaderArgs();

	@ArgGroup(exclusive = false, heading = "Processor options%n")
	KeyValueProcessorArgs processorArgs = new KeyValueProcessorArgs();

	@Override
	protected AbstractRedisRunnable runnable() {
		AbstractExport export = exportRunnable();
		export.setReaderOptions(readerArgs.readerOptions());
		export.setProcessorOptions(processorArgs.processorOptions());
		return export;
	}

	protected abstract AbstractExport exportRunnable();

	@Override
	protected LongSupplier initialMaxSupplier(String stepName, ItemReader<?> reader) {
		if (((RedisItemReader<?, ?, ?>) reader).getMode() == ReaderMode.LIVE) {
			return () -> ProgressStepExecutionListener.UNKNOWN_SIZE;
		}
		ScanSizeEstimator estimator = new ScanSizeEstimator(((RedisItemReader<?, ?, ?>) reader).getClient());
		estimator.setKeyPattern(readerArgs.scanMatch);
		if (readerArgs.scanType != null) {
			estimator.setKeyType(readerArgs.scanType.getCode());
		}
		return estimator;
	}

}
