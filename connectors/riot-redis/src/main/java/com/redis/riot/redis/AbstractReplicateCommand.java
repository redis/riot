package com.redis.riot.redis;

import org.springframework.batch.core.job.flow.Flow;
import org.springframework.batch.core.job.flow.support.SimpleFlow;
import org.springframework.batch.core.step.builder.FaultTolerantStepBuilder;
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
import com.redis.riot.RiotStepBuilder;
import com.redis.spring.batch.support.KeyValue;
import com.redis.spring.batch.support.LiveRedisItemReaderBuilder;
import com.redis.spring.batch.support.PollableItemReader;
import com.redis.spring.batch.support.job.JobFactory;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.extern.slf4j.Slf4j;
import picocli.CommandLine;

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
		return flow(jobFactory.step("skipped-verification-notification-step").tasklet((contribution, chunkContext) -> {
			log.info("Key processor enabled, skipping verification");
			return RepeatStatus.FINISHED;
		}).build());
	}

	private Flow replicationFlow(JobFactory jobFactory) {
		switch (replicationOptions.getMode()) {
		case LIVE:
			SimpleFlow notificationFlow = jobFactory.flow("notification-flow").start(liveStep(jobFactory).build())
					.build();
			SimpleFlow scanFlow = jobFactory.flow("scan-flow").start(scanStep(jobFactory)).build();
			return jobFactory.flow("live-flow").split(new SimpleAsyncTaskExecutor()).add(notificationFlow, scanFlow)
					.build();
		case LIVEONLY:
			return jobFactory.flow("live-only-flow").start(liveStep(jobFactory).build()).build();
		default:
			return jobFactory.flow("snapshot-flow").start(scanStep(jobFactory)).build();
		}
	}

	private TaskletStep scanStep(JobFactory jobFactory) {
		StepBuilder stepBuilder = jobFactory.step("scan-replication-step");
		RiotStepBuilder<T, T> scanStep = riotStep(stepBuilder, "Scanning");
		initialMax(scanStep);
		return scanStep.reader(reader(getRedisOptions())).processor(keyValueProcessor())
				.writer(writer(targetRedisOptions)).build().build();
	}

	private <KV extends KeyValue<String, ?>> ItemProcessor<KV, KV> keyValueProcessor() {
		return keyValueProcessorOptions.processor(getRedisOptions(), targetRedisOptions);
	}

	private FaultTolerantStepBuilder<T, T> liveStep(JobFactory jobFactory) {
		StepBuilder stepBuilder = jobFactory.step("live-replication-step");
		RiotStepBuilder<T, T> notificationStep = riotStep(stepBuilder, "Listening");
		return notificationStep.reader(liveReader(getRedisOptions())).processor(keyValueProcessor())
				.writer(writer(targetRedisOptions)).flushingOptions(flushingTransferOptions).build();
	}

	protected abstract ItemReader<T> reader(RedisOptions redisOptions);

	protected abstract PollableItemReader<T> liveReader(RedisOptions redisOptions);

	protected abstract ItemWriter<T> writer(RedisOptions redisOptions);

	@SuppressWarnings("unchecked")
	protected <B extends LiveRedisItemReaderBuilder<?, ?>> B configure(B builder) {
		log.debug("Configuring live reader with {}, queueCapacity={}", readerOptions,
				replicationOptions.getNotificationQueueCapacity());
		return (B) readerOptions.configure(builder.keyPatterns(readerOptions.getScanMatch())
				.queueCapacity(replicationOptions.getNotificationQueueCapacity())
				.database(getRedisOptions().uris().get(0).getDatabase())
				.flushingInterval(flushingTransferOptions.getFlushIntervalDuration())
				.idleTimeout(flushingTransferOptions.getIdleTimeoutDuration()));
	}

}
