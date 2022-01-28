package com.redis.riot.redis;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.listener.JobExecutionListenerSupport;
import org.springframework.batch.core.listener.StepExecutionListenerSupport;
import org.springframework.batch.core.step.builder.SimpleStepBuilder;

import com.redis.riot.AbstractTransferCommand;
import com.redis.riot.RedisOptions;
import com.redis.riot.RedisReaderOptions;
import com.redis.riot.RiotStepBuilder;
import com.redis.riot.TransferOptions;
import com.redis.spring.batch.DataStructure;
import com.redis.spring.batch.RedisItemReader;
import com.redis.spring.batch.compare.KeyComparisonItemWriter;
import com.redis.spring.batch.compare.KeyComparisonLogger;
import com.redis.spring.batch.compare.KeyComparisonResults;
import com.redis.spring.batch.reader.DataStructureIntrospectingValueReader;
import com.redis.spring.batch.support.RedisConnectionBuilder;

import io.lettuce.core.codec.StringCodec;
import picocli.CommandLine;

public abstract class AbstractTargetCommand extends AbstractTransferCommand {

	private static final Logger log = LoggerFactory.getLogger(AbstractTargetCommand.class);

	private static final String COMPARE_MESSAGE_ASCII = " >%,d T%,d ≠%,d ⧗%,d <%,d";
	private static final String COMPARE_MESSAGE_COLOR = " \u001b[31m>%,d \u001b[33mT%,d \u001b[35m≠%,d \u001b[36m⧗%,d\u001b[0m";
	private static final String VERIFICATION_NAME = "verification";

	@CommandLine.ArgGroup(exclusive = false, heading = "Target Redis connection options%n")
	protected RedisOptions targetRedisOptions = new RedisOptions();
	@CommandLine.ArgGroup(exclusive = false, heading = "Reader options%n")
	protected RedisReaderOptions readerOptions = new RedisReaderOptions();
	@CommandLine.Mixin
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
	protected JobBuilder configureJob(JobBuilder job) {
		return super.configureJob(job).listener(new JobExecutionListenerSupport() {

			@Override
			public void afterJob(JobExecution jobExecution) {
				targetRedisOptions.shutdown();
			}
		});
	}

	protected void initialMax(RiotStepBuilder<?, ?> step) {
		step.initialMax(readerOptions.initialMaxSupplier(estimator()));
	}

	protected Step verificationStep() throws Exception {
		RedisItemReader<String, DataStructure<String>> sourceReader = readerOptions
				.configureScanReader(configureJobRepository(reader(getRedisOptions()).dataStructureIntrospect()))
				.build();
		log.debug("Creating key comparator with TTL tolerance of {} seconds", compareOptions.getTtlTolerance());
		DataStructureIntrospectingValueReader targetValueReader = dataStructureValueReader(targetRedisOptions);
		KeyComparisonItemWriter<String> writer = KeyComparisonItemWriter.valueReader(targetValueReader)
				.tolerance(compareOptions.getTtlToleranceDuration()).build();
		if (compareOptions.isShowDiffs()) {
			writer.addListener(new KeyComparisonLogger(LoggerFactory.getLogger(getClass())));
		}
		RiotStepBuilder<DataStructure<String>, DataStructure<String>> stepBuilder = riotStep(VERIFICATION_NAME,
				"Verifying");
		initialMax(stepBuilder);
		stepBuilder.reader(sourceReader).writer(writer);
		stepBuilder.extraMessage(() -> extraMessage(writer.getResults()));
		SimpleStepBuilder<DataStructure<String>, DataStructure<String>> step = stepBuilder.build();
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
					log.debug("Verification interrupted");
					Thread.currentThread().interrupt();
					return ExitStatus.STOPPED;
				}
				KeyComparisonResults results = writer.getResults();
				log.warn("Verification failed: OK={} Missing={} Values={} TTLs={} Types={}", results.getOK(),
						results.getMissing(), results.getValue(), results.getTTL(), results.getType());
				return new ExitStatus(ExitStatus.FAILED.getExitCode(), "Verification failed");
			}
		});
		return step.build();
	}

	private DataStructureIntrospectingValueReader dataStructureValueReader(RedisOptions redisOptions) {
		RedisConnectionBuilder<String, String, ?> builder = new RedisConnectionBuilder<>(redisOptions.client(),
				StringCodec.UTF8);
		return new DataStructureIntrospectingValueReader(builder.connectionSupplier(), readerOptions.poolConfig(),
				builder.async());
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
