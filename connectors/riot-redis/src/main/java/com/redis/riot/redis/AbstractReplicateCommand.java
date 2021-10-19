package com.redis.riot.redis;

import org.springframework.batch.core.job.flow.Flow;
import org.springframework.batch.core.job.flow.support.SimpleFlow;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.core.step.tasklet.TaskletStep;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.core.task.SimpleAsyncTaskExecutor;

import com.redis.riot.FlushingTransferOptions;
import com.redis.riot.KeyValueProcessorOptions;
import com.redis.riot.RedisOptions;
import com.redis.riot.RedisWriterOptions;
import com.redis.riot.RiotStepBuilder;
import com.redis.spring.batch.RedisItemWriter.RedisItemWriterBuilder;
import com.redis.spring.batch.support.KeyValue;
import com.redis.spring.batch.support.LiveRedisItemReaderBuilder;
import com.redis.spring.batch.support.ScanRedisItemReaderBuilder;
import com.redis.spring.batch.support.job.JobFactory;

import io.lettuce.core.AbstractRedisClient;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.extern.slf4j.Slf4j;
import picocli.CommandLine;
import picocli.CommandLine.ArgGroup;

@Data
@EqualsAndHashCode(callSuper = true)
@Slf4j
public abstract class AbstractReplicateCommand<T extends KeyValue<String, ?>> extends AbstractTargetCommand {

	@CommandLine.Mixin
	private FlushingTransferOptions flushingTransferOptions = new FlushingTransferOptions();
	@CommandLine.Mixin
	private ReplicationOptions replicationOptions = new ReplicationOptions();
	@CommandLine.Mixin
	protected KeyValueProcessorOptions keyValueProcessorOptions = new KeyValueProcessorOptions();
	@ArgGroup(exclusive = false, heading = "Writer options%n")
	private RedisWriterOptions writerOptions = new RedisWriterOptions();

	@Override
	protected Flow flow(JobFactory jobFactory) {
		if (replicationOptions.isVerify()) {
			return jobFactory.flow("replication-verification-flow").start(replicationFlow(jobFactory))
					.next(verificationFlow(jobFactory)).build();
		}
		return replicationFlow(jobFactory);
	}

	@Override
	protected Flow verificationFlow(JobFactory jobFactory) {
		if (keyValueProcessor() == null) {
			return super.verificationFlow(jobFactory);
		}
		// Verification cannot be done if a processor is set
		return flow("skipped-verification-notification-flow",
				jobFactory.step("skipped-verification-notification-step").tasklet((contribution, chunkContext) -> {
					log.info("Key processor enabled, skipping verification");
					return RepeatStatus.FINISHED;
				}).build());
	}

	private Flow replicationFlow(JobFactory jobFactory) {
		switch (replicationOptions.getMode()) {
		case LIVE:
			SimpleFlow notificationFlow = jobFactory.flow("notification-flow").start(liveStep(jobFactory)).build();
			SimpleFlow scanFlow = jobFactory.flow("scan-flow").start(scanStep(jobFactory)).build();
			return jobFactory.flow("live-flow").split(new SimpleAsyncTaskExecutor()).add(notificationFlow, scanFlow)
					.build();
		case LIVEONLY:
			return jobFactory.flow("live-only-flow").start(liveStep(jobFactory)).build();
		default:
			return jobFactory.flow("snapshot-flow").start(scanStep(jobFactory)).build();
		}
	}

	private TaskletStep scanStep(JobFactory jobFactory) {
		StepBuilder stepBuilder = jobFactory.step("scan-replication-step");
		RiotStepBuilder<T, T> scanStep = riotStep(stepBuilder, "Scanning");
		initialMax(scanStep);
		scanStep.reader(reader());
		configure(scanStep);
		return scanStep.build().build();
	}

	private <KV extends KeyValue<String, ?>> ItemProcessor<KV, KV> keyValueProcessor() {
		return keyValueProcessorOptions.processor(getRedisOptions(), targetRedisOptions);
	}

	private TaskletStep liveStep(JobFactory jobFactory) {
		StepBuilder stepBuilder = jobFactory.step("live-replication-step");
		RiotStepBuilder<T, T> liveStep = riotStep(stepBuilder, "Listening");
		liveStep.reader(liveReader());
		liveStep.flushingOptions(flushingTransferOptions);
		configure(liveStep);
		return liveStep.build().build();
	}

	private void configure(RiotStepBuilder<T, T> step) {
		step.processor(keyValueProcessor()).writer(writer());
	}

	private ItemWriter<T> writer() {
		log.info("Configuring writer with {}", targetRedisOptions);
		return writerOptions
				.configure(writer(targetRedisOptions.client()).poolConfig(poolConfig(writerOptions.getPoolMax())))
				.build();
	}

	private ItemReader<T> reader() {
		RedisOptions sourceRedisOptions = getRedisOptions();
		log.info("Configuring reader with {} {}", sourceRedisOptions, readerOptions);
		return readerOptions
				.configure(reader(sourceRedisOptions.client()).poolConfig(poolConfig(readerOptions.getPoolMax())))
				.build();
	}

	private ItemReader<T> liveReader() {
		RedisOptions sourceRedisOptions = getRedisOptions();
		log.info("Configuring live reader with {} {} {} {}", sourceRedisOptions, readerOptions, replicationOptions,
				flushingTransferOptions);
		return readerOptions.configure(liveReader(sourceRedisOptions.client())
				.poolConfig(poolConfig(readerOptions.getPoolMax())).keyPatterns(readerOptions.getScanMatch())
				.queueCapacity(replicationOptions.getNotificationQueueCapacity())
				.database(sourceRedisOptions.uris().get(0).getDatabase())
				.flushingInterval(flushingTransferOptions.getFlushIntervalDuration())
				.idleTimeout(flushingTransferOptions.getIdleTimeoutDuration())).build();
	}

	protected abstract ScanRedisItemReaderBuilder<T, ?> reader(AbstractRedisClient client);

	protected abstract LiveRedisItemReaderBuilder<T, ?> liveReader(AbstractRedisClient client);

	protected abstract RedisItemWriterBuilder<String, String, T> writer(AbstractRedisClient client);

}
