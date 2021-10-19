package com.redis.riot.redis;

import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.batch.core.listener.StepExecutionListenerSupport;
import org.springframework.batch.core.step.builder.SimpleStepBuilder;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.core.step.tasklet.TaskletStep;

import com.redis.riot.AbstractTransferCommand;
import com.redis.riot.RedisOptions;
import com.redis.riot.RedisReaderOptions;
import com.redis.riot.RiotStepBuilder;
import com.redis.riot.TransferOptions;
import com.redis.spring.batch.RedisItemReader;
import com.redis.spring.batch.support.DataStructure;
import com.redis.spring.batch.support.DataStructureValueReader;
import com.redis.spring.batch.support.KeyComparisonItemWriter;
import com.redis.spring.batch.support.KeyComparisonItemWriter.KeyComparisonItemWriterBuilder;
import com.redis.spring.batch.support.KeyComparisonMismatchPrinter;
import com.redis.spring.batch.support.KeyComparisonResultCounter;
import com.redis.spring.batch.support.job.JobFactory;

import io.lettuce.core.AbstractRedisClient;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.extern.slf4j.Slf4j;
import picocli.CommandLine;

@Data
@EqualsAndHashCode(callSuper = true)
@Slf4j
public abstract class AbstractTargetCommand extends AbstractTransferCommand {

	private static final String ASCII_COMPARE_MESSAGE_FORMAT = ">%,d T%,d ≠%,d ⧗%,d <%,d";
	private static final String COLORFUL_COMPARE_MESSAGE_FORMAT = "\u001b[31m>%,d \u001b[33mT%,d \u001b[35m≠%,d \u001b[36m⧗%,d\u001b[0m";

	@CommandLine.ArgGroup(exclusive = false, heading = "Target Redis connection options%n")
	protected RedisOptions targetRedisOptions = new RedisOptions();
	@CommandLine.ArgGroup(exclusive = false, heading = "Source Redis reader options%n")
	protected RedisReaderOptions readerOptions = new RedisReaderOptions();
	@CommandLine.Mixin
	private CompareOptions compareOptions = new CompareOptions();

	@Override
	public void afterJob(JobExecution jobExecution) {
		targetRedisOptions.shutdown();
		super.afterJob(jobExecution);
	}

	protected void initialMax(RiotStepBuilder<?, ?> step) {
		step.initialMax(readerOptions.initialMaxSupplier(getRedisOptions()));
	}

	protected Flow verificationFlow(JobFactory jobFactory) {
		RedisItemReader<String, DataStructure<String>> sourceReader = dataStructureReader();
		log.debug("Creating key comparator with TTL tolerance of {} seconds", compareOptions.getTtlTolerance());
		DataStructureValueReader<String, String> targetValueReader = targetDataStructureValueReader();
		KeyComparisonResultCounter<String> counter = new KeyComparisonResultCounter<>();
		KeyComparisonItemWriterBuilder<String> writerBuilder = KeyComparisonItemWriter.valueReader(targetValueReader);
		writerBuilder.resultHandler(counter);
		if (compareOptions.isShowDiffs()) {
			writerBuilder.resultHandler(new KeyComparisonMismatchPrinter<>());
		}
		writerBuilder.ttlTolerance(compareOptions.getTtlToleranceDuration());
		KeyComparisonItemWriter<String> writer = writerBuilder.build();
		StepBuilder verificationStepBuilder = jobFactory.step("verification-step");
		RiotStepBuilder<DataStructure<String>, DataStructure<String>> stepBuilder = riotStep(verificationStepBuilder,
				"Verifying");
		initialMax(stepBuilder);
		stepBuilder.reader(sourceReader).writer(writer);
		stepBuilder.extraMessage(() -> extraMessage(counter));
		SimpleStepBuilder<DataStructure<String>, DataStructure<String>> step = stepBuilder.build();
		step.listener(new StepExecutionListenerSupport() {
			@Override
			public ExitStatus afterStep(StepExecution stepExecution) {
				if (counter.isOK()) {
					log.info("Verification completed - all OK");
					return super.afterStep(stepExecution);
				}
				try {
					Thread.sleep(getTransferOptions().getProgressUpdateIntervalMillis());
				} catch (InterruptedException e) {
					log.debug("Verification interrupted");
					return null;
				}
				log.warn("Verification failed: OK={} Missing={} Values={} TTLs={} Types={}",
						(Object[]) counter.get(KeyComparisonItemWriter.Status.OK,
								KeyComparisonItemWriter.Status.MISSING, KeyComparisonItemWriter.Status.VALUE,
								KeyComparisonItemWriter.Status.TTL, KeyComparisonItemWriter.Status.TYPE));
				return new ExitStatus(ExitStatus.FAILED.getExitCode(), "Verification failed");
			}
		});
		TaskletStep verificationStep = step.build();
		return jobFactory.flow("verification-flow").start(verificationStep).build();
	}

	private String extraMessage(KeyComparisonResultCounter<String> counter) {
		Long[] counts = counter.get(KeyComparisonItemWriter.Status.MISSING, KeyComparisonItemWriter.Status.TYPE,
				KeyComparisonItemWriter.Status.VALUE, KeyComparisonItemWriter.Status.TTL);
		return " " + String.format(extraMessageFormat(), (Object[]) counts);
	}

	private String extraMessageFormat() {
		if (getTransferOptions().getProgress() == TransferOptions.Progress.COLOR) {
			return COLORFUL_COMPARE_MESSAGE_FORMAT;
		}
		return ASCII_COMPARE_MESSAGE_FORMAT;
	}

	protected RedisItemReader<String, DataStructure<String>> dataStructureReader() {
		AbstractRedisClient client = getRedisOptions().client();
		return readerOptions.configure(RedisItemReader.dataStructure(client).poolConfig(getRedisOptions().poolConfig()))
				.build();
	}

	protected DataStructureValueReader<String, String> targetDataStructureValueReader() {
		return DataStructureValueReader.client(targetRedisOptions.client()).poolConfig(targetRedisOptions.poolConfig())
				.build();
	}

}
