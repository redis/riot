package com.redis.riot.redis;

import java.util.logging.Level;
import java.util.logging.Logger;

import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.listener.StepExecutionListenerSupport;
import org.springframework.batch.core.step.builder.SimpleStepBuilder;

import com.redis.riot.AbstractTransferCommand;
import com.redis.riot.JobCommandContext;
import com.redis.riot.RedisOptions;
import com.redis.riot.RedisReaderOptions;
import com.redis.riot.RiotStep;
import com.redis.riot.RiotStep.Builder;
import com.redis.riot.TransferOptions;
import com.redis.spring.batch.DataStructure;
import com.redis.spring.batch.RedisItemReader;
import com.redis.spring.batch.RedisScanSizeEstimator;
import com.redis.spring.batch.compare.KeyComparisonItemWriter;
import com.redis.spring.batch.compare.KeyComparisonLogger;
import com.redis.spring.batch.compare.KeyComparisonResults;

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

	public RedisReaderOptions getReaderOptions() {
		return readerOptions;
	}

	public CompareOptions getCompareOptions() {
		return compareOptions;
	}

	@Override
	protected Job createJob(JobCommandContext context) throws Exception {
		return createJob(new TargetCommandContext(context, targetRedisOptions));
	}

	protected abstract Job createJob(TargetCommandContext context);

	@Override
	protected RedisScanSizeEstimator.Builder estimator(JobCommandContext context) {
		return super.estimator(context).match(readerOptions.getScanMatch()).sampleSize(readerOptions.getSampleSize())
				.type(readerOptions.getScanType());
	}

	protected Step verificationStep(TargetCommandContext context) {
		RedisItemReader<String, DataStructure<String>> sourceReader = readerOptions
				.configure(RedisItemReader.dataStructure(context.getRedisClient())).jobRunner(context.getJobRunner())
				.build();
		RedisItemReader<String, DataStructure<String>> targetReader = readerOptions
				.configure(RedisItemReader.dataStructure(context.getTargetRedisClient())).build();
		log.log(Level.FINE, "Creating key comparator with TTL tolerance of {0} seconds",
				compareOptions.getTtlTolerance());
		KeyComparisonItemWriter writer = KeyComparisonItemWriter.valueReader(targetReader.getValueReader())
				.tolerance(compareOptions.getTtlToleranceDuration()).build();
		if (compareOptions.isShowDiffs()) {
			writer.addListener(new KeyComparisonLogger(java.util.logging.Logger.getLogger(getClass().getName())));
		}
		RedisScanSizeEstimator estimator = estimator(context).build();
		Builder<DataStructure<String>, DataStructure<String>> riotStep = RiotStep.reader(sourceReader).writer(writer)
				.taskName("Verifying").max(estimator::execute).message(() -> extraMessage(writer.getResults()));
		SimpleStepBuilder<DataStructure<String>, DataStructure<String>> step = step(
				context.getJobRunner().step(VERIFICATION_NAME), riotStep.build());
		step.listener(new StepExecutionListenerSupport() {
			@Override
			public ExitStatus afterStep(StepExecution stepExecution) {
				if (stepExecution.getStatus().isUnsuccessful()) {
					return null;
				}
				if (writer.getResults().isOK()) {
					log.info("Verification completed - all OK");
					return ExitStatus.COMPLETED;
				}
				try {
					Thread.sleep(transferOptions.getProgressUpdateIntervalMillis());
				} catch (InterruptedException e) {
					log.fine("Verification interrupted");
					Thread.currentThread().interrupt();
					return ExitStatus.STOPPED;
				}
				KeyComparisonResults results = writer.getResults();
				log.log(Level.WARNING, "Verification failed: OK={0} Missing={1} Values={2} TTLs={3} Types={4}",
						new Object[] { results.getOK(), results.getMissing(), results.getValue(), results.getTTL(),
								results.getType() });
				return new ExitStatus(ExitStatus.FAILED.getExitCode(), "Verification failed");
			}
		});
		return step.build();
	}

	private String extraMessage(KeyComparisonResults results) {
		return String.format(extraMessageFormat(), results.getMissing(), results.getType(), results.getValue(),
				results.getTTL());
	}

	private String extraMessageFormat() {
		if (transferOptions.getProgress() == TransferOptions.Progress.COLOR) {
			return COMPARE_MESSAGE_COLOR;
		}
		return COMPARE_MESSAGE_ASCII;
	}

}
