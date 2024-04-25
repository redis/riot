package com.redis.riot.cli;

import java.util.concurrent.Callable;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.ItemWriteListener;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.batch.core.step.builder.SimpleStepBuilder;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.util.ClassUtils;

import com.redis.riot.core.AbstractRiotCallable;
import com.redis.riot.faker.FakerItemReader;
import com.redis.spring.batch.RedisItemReader;
import com.redis.spring.batch.RedisItemReader.ReaderMode;
import com.redis.spring.batch.gen.GeneratorItemReader;
import com.redis.spring.batch.reader.ScanSizeEstimator;

import me.tongfei.progressbar.DelegatingProgressBarConsumer;
import me.tongfei.progressbar.ProgressBarBuilder;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(usageHelpAutoWidth = true, abbreviateSynopsis = true)
abstract class AbstractRiotCommand implements Callable<Integer> {

	public enum ProgressStyle {
		BLOCK, BAR, ASCII, LOG, NONE
	}

	@Option(names = "--help", usageHelp = true, description = "Show this help message and exit.")
	private boolean helpRequested;

	@ArgGroup(exclusive = false, heading = "Logging options%n")
	private LoggingArgs loggingArgs = new LoggingArgs();

	@ArgGroup(exclusive = false, heading = "Job options%n")
	private JobArgs jobArgs = new JobArgs();

	private String name;

	public void setName(String name) {
		this.name = name;
	}

	@Override
	public Integer call() throws Exception {
		loggingArgs.configureLogging();
		try (AbstractRiotCallable callable = callable()) {
			if (name != null) {
				callable.setName(name);
			}
			jobArgs.configure(callable);
			if (jobArgs.getProgressStyle() != ProgressStyle.NONE) {
				callable.addStepConfiguration(this::configureProgress);
			}
			callable.afterPropertiesSet();
			callable.call();
		}
		return 0;
	}

	private void configureProgress(SimpleStepBuilder<?, ?> step, String stepName, ItemReader<?> reader,
			ItemWriter<?> writer) {
		ProgressBarBuilder progressBar = new ProgressBarBuilder();
		progressBar.setTaskName(taskName(stepName));
		progressBar.setStyle(jobArgs.progressBarStyle());
		progressBar.setUpdateIntervalMillis(jobArgs.getProgressUpdateInterval());
		progressBar.showSpeed();
		if (jobArgs.getProgressStyle() == ProgressStyle.LOG) {
			Logger logger = LoggerFactory.getLogger(getClass());
			progressBar.setConsumer(new DelegatingProgressBarConsumer(logger::info));
		}
		ProgressStepExecutionListener listener = new ProgressStepExecutionListener(progressBar);
		listener.setExtraMessage(extraMessage(stepName, reader, writer));
		listener.setInitialMax(initialMax(reader));
		step.listener((StepExecutionListener) listener);
		step.listener((ItemWriteListener<?>) listener);
	}

	@SuppressWarnings("rawtypes")
	private LongSupplier initialMax(ItemReader<?> reader) {
		if (reader instanceof RedisItemReader) {
			RedisItemReader redisReader = (RedisItemReader) reader;
			if (redisReader.getMode() == ReaderMode.SNAPSHOT) {
				ScanSizeEstimator estimator = new ScanSizeEstimator(redisReader.getClient());
				estimator.setKeyPattern(redisReader.getKeyPattern());
				estimator.setKeyType(redisReader.getKeyType());
				return estimator;
			}
		}
		if (reader instanceof GeneratorItemReader) {
			GeneratorItemReader generatorReader = (GeneratorItemReader) reader;
			return () -> generatorReader.getMaxItemCount() - generatorReader.getCurrentItemCount();
		}
		if (reader instanceof FakerItemReader) {
			FakerItemReader fakerReader = (FakerItemReader) reader;
			return () -> fakerReader.getMaxItemCount() - fakerReader.getCurrentItemCount();
		}
		return null;
	}

	protected String taskName(String stepName) {
		return ClassUtils.getShortName(getClass());
	}

	protected Supplier<String> extraMessage(String stepName, ItemReader<?> reader, ItemWriter<?> writer) {
		return null;
	}

	protected abstract AbstractRiotCallable callable();

	public LoggingArgs getLoggingArgs() {
		return loggingArgs;
	}

	public void setLoggingArgs(LoggingArgs loggingMixin) {
		this.loggingArgs = loggingMixin;
	}

	public JobArgs getJobArgs() {
		return jobArgs;
	}

	public void setJobArgs(JobArgs jobArgs) {
		this.jobArgs = jobArgs;
	}

}
