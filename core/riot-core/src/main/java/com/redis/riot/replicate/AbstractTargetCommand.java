package com.redis.riot.replicate;

import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.ItemWriteListener;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.listener.StepExecutionListenerSupport;
import org.springframework.batch.core.step.builder.SimpleStepBuilder;

import com.redis.riot.AbstractTransferCommand;
import com.redis.riot.JobCommandContext;
import com.redis.riot.ProgressMonitor;
import com.redis.riot.ProgressStyle;
import com.redis.riot.RedisOptions;
import com.redis.riot.RedisReaderOptions;
import com.redis.spring.batch.RedisItemReader;
import com.redis.spring.batch.common.JobRunner;
import com.redis.spring.batch.reader.KeyComparatorOptions;
import com.redis.spring.batch.reader.KeyComparison;
import com.redis.spring.batch.reader.KeyComparison.Status;
import com.redis.spring.batch.reader.ReaderOptions;
import com.redis.spring.batch.reader.ScanSizeEstimator;
import com.redis.spring.batch.writer.KeyComparisonCountItemWriter;
import com.redis.spring.batch.writer.KeyComparisonCountItemWriter.Results;
import com.redis.spring.batch.writer.KeyComparisonLogger;

import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Mixin;

public abstract class AbstractTargetCommand extends AbstractTransferCommand {

	private static final Logger log = Logger.getLogger(AbstractTargetCommand.class.getName());

	private static final String COMPARE_MESSAGE_ASCII = " >%,d T%,d ≠%,d ⧗%,d <%,d";
	private static final String COMPARE_MESSAGE_COLOR = " \u001b[31m>%,d \u001b[33mT%,d \u001b[35m≠%,d \u001b[36m⧗%,d\u001b[0m";
	private static final String VERIFICATION_NAME = "verification";

	@ArgGroup(exclusive = false, heading = "Target Redis connection options%n")
	private RedisOptions targetRedisOptions = new RedisOptions();

	@ArgGroup(exclusive = false, heading = "Reader options%n")
	protected RedisReaderOptions readerOptions = new RedisReaderOptions();

	@Mixin
	private CompareOptions compareOptions = new CompareOptions();

	public RedisOptions getTargetRedisOptions() {
		return targetRedisOptions;
	}

	public void setTargetRedisOptions(RedisOptions targetRedisOptions) {
		this.targetRedisOptions = targetRedisOptions;
	}

	public RedisReaderOptions getReaderOptions() {
		return readerOptions;
	}

	public void setReaderOptions(RedisReaderOptions readerOptions) {
		this.readerOptions = readerOptions;
	}

	public CompareOptions getCompareOptions() {
		return compareOptions;
	}

	@Override
	protected JobCommandContext context(JobRunner jobRunner, RedisOptions redisOptions) {
		return new TargetCommandContext(jobRunner, redisOptions, targetRedisOptions);
	}

	protected ScanSizeEstimator estimator(JobCommandContext context) {
		return ScanSizeEstimator.client(context.getRedisClient()).options(readerOptions.scanSizeEstimatorOptions())
				.build();
	}

	private static class KeyComparisonWriteListener implements ItemWriteListener<KeyComparison> {

		private final KeyComparisonLogger logger;

		public KeyComparisonWriteListener(KeyComparisonLogger logger) {
			this.logger = logger;
		}

		@Override
		public void onWriteError(Exception exception, List<? extends KeyComparison> items) {
			// do nothing
		}

		@Override
		public void beforeWrite(List<? extends KeyComparison> items) {
			// do nothing
		}

		@Override
		public void afterWrite(List<? extends KeyComparison> items) {
			items.forEach(logger::log);
		}
	}

	private class KeyComparisonStepListener extends StepExecutionListenerSupport {

		private final KeyComparisonCountItemWriter writer;

		public KeyComparisonStepListener(KeyComparisonCountItemWriter writer) {
			this.writer = writer;
		}

		@Override
		public ExitStatus afterStep(StepExecution stepExecution) {
			if (stepExecution.getStatus().isUnsuccessful()) {
				return null;
			}
			Results results = writer.getResults();
			if (results.getTotalCount() == results.getCount(Status.OK)) {
				log.info("Verification completed - all OK");
				return ExitStatus.COMPLETED;
			}
			try {
				Thread.sleep(getTransferOptions().getProgressUpdateInterval());
			} catch (InterruptedException e) {
				log.fine("Verification interrupted");
				Thread.currentThread().interrupt();
				return ExitStatus.STOPPED;
			}
			log.log(Level.WARNING, "Verification failed: OK={0} Missing={1} Values={2} TTLs={3} Types={4}",
					new Object[] { results.getCount(Status.OK), results.getCount(Status.MISSING),
							results.getCount(Status.VALUE), results.getCount(Status.TTL),
							results.getCount(Status.TYPE) });
			return new ExitStatus(ExitStatus.FAILED.getExitCode(), "Verification failed");
		}
	}

	protected Step verificationStep(TargetCommandContext context) {
		log.log(Level.FINE, "Creating key comparator with TTL tolerance of {0} seconds",
				compareOptions.getTtlTolerance());
		RedisItemReader<String, KeyComparison> reader = context.reader().comparator(context.getTargetRedisClient())
				.comparatorOptions(comparatorOptions()).build();
		KeyComparisonCountItemWriter writer = new KeyComparisonCountItemWriter();
		SimpleStepBuilder<KeyComparison, KeyComparison> step = step(context, VERIFICATION_NAME, reader, null, writer);
		if (compareOptions.isShowDiffs()) {
			step.listener(new KeyComparisonWriteListener(new KeyComparisonLogger(log)));
		}
		step.listener(new KeyComparisonStepListener(writer));
		ProgressMonitor monitor = progressMonitor().task("Verifying").initialMax(estimator(context)::execute)
				.extraMessage(() -> extraMessage(writer.getResults())).build();
		return step(step, monitor).build();
	}

	private KeyComparatorOptions comparatorOptions() {
		ReaderOptions leftOptions = readerOptions.readerOptions();
		return KeyComparatorOptions.builder().leftPoolOptions(leftOptions.getPoolOptions())
				.rightPoolOptions(leftOptions.getPoolOptions()).scanOptions(readerOptions.scanOptions())
				.ttlTolerance(compareOptions.getTtlToleranceDuration()).build();
	}

	private String extraMessage(Results results) {
		return String.format(extraMessageFormat(), results.getCount(Status.MISSING), results.getCount(Status.TYPE),
				results.getCount(Status.VALUE), results.getCount(Status.TTL));
	}

	private String extraMessageFormat() {
		ProgressStyle progressStyle = getTransferOptions().getProgressStyle();
		if (progressStyle == ProgressStyle.BAR || progressStyle == ProgressStyle.BLOCK) {
			return COMPARE_MESSAGE_COLOR;
		}
		return COMPARE_MESSAGE_ASCII;
	}

}
