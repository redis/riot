package com.redis.riot.cli;

import java.util.function.LongSupplier;

import org.springframework.batch.item.ItemReader;

import com.redis.riot.core.AbstractExport;
import com.redis.riot.core.AbstractRedisCallable;
import com.redis.spring.batch.RedisItemReader;
import com.redis.spring.batch.RedisItemReader.ReaderMode;
import com.redis.spring.batch.reader.ScanSizeEstimator;

import picocli.CommandLine.ArgGroup;

public abstract class AbstractExportCommand extends AbstractRiotCommand {

	@ArgGroup(exclusive = false)
	private RedisReaderArgs readerArgs = new RedisReaderArgs();

	@ArgGroup(exclusive = false)
	private KeyValueProcessorArgs processorArgs = new KeyValueProcessorArgs();

	@ArgGroup(exclusive = false)
	private KeyFilterArgs keyFilterArgs = new KeyFilterArgs();

	@Override
	protected AbstractRedisCallable runnable() {
		AbstractExport export = exportRunnable();
		export.setReaderOptions(readerArgs.readerOptions());
		export.setProcessorOptions(processorArgs.processorOptions());
		export.setKeyFilterOptions(keyFilterArgs.keyFilterOptions());
		return export;
	}

	protected abstract AbstractExport exportRunnable();

	@Override
	protected LongSupplier initialMaxSupplier(String stepName, ItemReader<?> reader) {
		if (((RedisItemReader<?, ?, ?>) reader).getMode() == ReaderMode.LIVE) {
			return () -> ProgressStepExecutionListener.UNKNOWN_SIZE;
		}
		ScanSizeEstimator estimator = new ScanSizeEstimator(((RedisItemReader<?, ?, ?>) reader).getClient());
		estimator.setKeyPattern(readerArgs.getScanMatch());
		if (readerArgs.getScanType() != null) {
			estimator.setKeyType(readerArgs.getScanType());
		}
		return estimator;
	}

	public RedisReaderArgs getReaderArgs() {
		return readerArgs;
	}

	public void setReaderArgs(RedisReaderArgs readerArgs) {
		this.readerArgs = readerArgs;
	}

	public KeyValueProcessorArgs getProcessorArgs() {
		return processorArgs;
	}

	public void setProcessorArgs(KeyValueProcessorArgs processorArgs) {
		this.processorArgs = processorArgs;
	}

	public KeyFilterArgs getKeyFilterArgs() {
		return keyFilterArgs;
	}

	public void setKeyFilterArgs(KeyFilterArgs keyFilterArgs) {
		this.keyFilterArgs = keyFilterArgs;
	}

}
