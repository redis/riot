package com.redis.riot.redis;

import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.batch.core.job.flow.support.SimpleFlow;
import org.springframework.batch.core.step.tasklet.TaskletStep;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.core.task.SimpleAsyncTaskExecutor;

import com.redis.riot.FlushingTransferOptions;
import com.redis.riot.KeyValueProcessorOptions;
import com.redis.riot.RedisWriterOptions;
import com.redis.riot.RiotStepBuilder;
import com.redis.spring.batch.RedisItemReader;
import com.redis.spring.batch.RedisItemReader.ItemReaderBuilder;
import com.redis.spring.batch.RedisItemWriter.BaseRedisItemWriterBuilder;
import com.redis.spring.batch.RedisItemWriter.OperationItemWriterBuilder;
import com.redis.spring.batch.builder.ScanRedisItemReaderBuilder;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.extern.slf4j.Slf4j;
import picocli.CommandLine;
import picocli.CommandLine.ArgGroup;

@Data
@EqualsAndHashCode(callSuper = true)
@Slf4j
@CommandLine.Command(name = "replicate", description = "Replicate a source Redis DB to a target Redis DB")
public class ReplicateCommand extends AbstractTargetCommand {

	private static final String SKIPPED_VERIFICATION_NAME = "skipped-verification-notification";

	private static final String LIVE_REPLICATION_NAME = "live-replication";

	private static final String SCAN_REPLICATION_NAME = "scan-replication";

	private static final String REPLICATION_NAME = "replication";

	@CommandLine.Mixin
	private FlushingTransferOptions flushingTransferOptions = new FlushingTransferOptions();
	@CommandLine.Mixin
	private ReplicationOptions replicationOptions = new ReplicationOptions();
	@CommandLine.Mixin
	private KeyValueProcessorOptions processorOptions = new KeyValueProcessorOptions();
	@ArgGroup(exclusive = false, heading = "Writer options%n")
	private RedisWriterOptions writerOptions = new RedisWriterOptions();

	@Override
	protected Flow flow() throws Exception {
		if (replicationOptions.isVerify()) {
			return new FlowBuilder<SimpleFlow>(REPLICATION_NAME).start(replicationFlow()).next(verificationFlow())
					.build();
		}
		return replicationFlow();
	}

	@Override
	protected Flow verificationFlow() throws Exception {
		if (processorOptions.getKeyProcessor() == null) {
			return super.verificationFlow();
		}
		// Verification cannot be done if a processor is set
		return flow(SKIPPED_VERIFICATION_NAME, step(SKIPPED_VERIFICATION_NAME).tasklet((contribution, chunkContext) -> {
			log.info("Key processor enabled, skipping verification");
			return RepeatStatus.FINISHED;
		}).build());
	}

	private Flow replicationFlow() throws Exception {
		switch (replicationOptions.getMode()) {
		case LIVE:
			SimpleFlow notificationFlow = new FlowBuilder<SimpleFlow>(LIVE_REPLICATION_NAME).start(liveStep()).build();
			SimpleFlow scanFlow = new FlowBuilder<SimpleFlow>(SCAN_REPLICATION_NAME).start(scanStep()).build();
			return new FlowBuilder<SimpleFlow>(REPLICATION_NAME).split(new SimpleAsyncTaskExecutor())
					.add(notificationFlow, scanFlow).build();
		case LIVEONLY:
			return new FlowBuilder<SimpleFlow>(LIVE_REPLICATION_NAME).start(liveStep()).build();
		default:
			return new FlowBuilder<SimpleFlow>(SCAN_REPLICATION_NAME).start(scanStep()).build();
		}
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	private TaskletStep scanStep() throws Exception {
		RedisItemReader reader = reader().build();
		reader.setName("redis-scan-reader");
		RiotStepBuilder scanStep = riotStep(SCAN_REPLICATION_NAME, "Scanning");
		initialMax(scanStep);
		return configure(scanStep.reader(reader)).build().build();
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	private TaskletStep liveStep() throws Exception {
		RedisItemReader reader = reader().live().keyPatterns(readerOptions.getScanMatch())
				.notificationQueueCapacity(replicationOptions.getNotificationQueueCapacity())
				.database(getRedisOptions().uris().get(0).getDatabase())
				.flushingInterval(flushingTransferOptions.getFlushIntervalDuration())
				.idleTimeout(flushingTransferOptions.getIdleTimeoutDuration()).build();
		reader.setName("redis--live-reader");
		return configure(
				riotStep(LIVE_REPLICATION_NAME, "Listening").reader(reader).flushingOptions(flushingTransferOptions))
						.build().build();
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	private RiotStepBuilder configure(RiotStepBuilder step) {
		return step.processor(processorOptions.processor(getRedisOptions(), targetRedisOptions)).writer(writer());
	}

	@SuppressWarnings("rawtypes")
	private ItemWriter writer() {
		log.info("Configuring writer with {}", targetRedisOptions);
		OperationItemWriterBuilder<String, String> writer = writer(targetRedisOptions);
		return writerOptions.configureWriter(redisWriter(writer)).build();
	}

	@SuppressWarnings("rawtypes")
	private BaseRedisItemWriterBuilder redisWriter(OperationItemWriterBuilder<String, String> builder) {
		switch (replicationOptions.getType()) {
		case DS:
			return builder.dataStructure();
		case DUMP:
			return builder.keyDump();
		default:
			break;
		}
		throw new IllegalArgumentException("Unknown replication type: " + replicationOptions.getType());
	}

	@SuppressWarnings("rawtypes")
	private ScanRedisItemReaderBuilder reader() throws Exception {
		return readerOptions.configureScanReader(configureJobRepository(reader(reader(getRedisOptions()))));
	}

	@SuppressWarnings("rawtypes")
	private ScanRedisItemReaderBuilder reader(ItemReaderBuilder reader) {
		switch (replicationOptions.getType()) {
		case DS:
			return reader.dataStructure();
		case DUMP:
			return reader.keyDump();
		default:
			break;
		}
		throw new IllegalArgumentException("Unknown replication type: " + replicationOptions.getType());
	}

}
