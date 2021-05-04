package com.redislabs.riot.redis;

import com.redislabs.riot.AbstractFlushingTransferCommand;
import com.redislabs.riot.RedisOptions;
import com.redislabs.riot.RedisReaderOptions;
import com.redislabs.riot.RiotStepBuilder;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.batch.core.job.flow.support.SimpleFlow;
import org.springframework.batch.core.step.builder.SimpleStepBuilder;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.core.step.tasklet.TaskletStep;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.redis.DataStructureItemReader;
import org.springframework.batch.item.redis.support.*;
import org.springframework.batch.item.support.AbstractItemStreamItemReader;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.util.ClassUtils;
import picocli.CommandLine;

import java.time.Duration;

@Slf4j
public abstract class AbstractReplicateCommand<T extends KeyValue<String, ?>> extends AbstractFlushingTransferCommand {

    @Getter
    @Setter
    @CommandLine.ArgGroup(exclusive = false, heading = "Target Redis connection options%n")
    private RedisOptions targetRedisOptions = RedisOptions.builder().build();
    @Getter
    @Setter
    @CommandLine.ArgGroup(exclusive = false, heading = "Source Redis reader options%n")
    private RedisReaderOptions readerOptions = RedisReaderOptions.builder().build();
    @Getter
    @Setter
    @SuppressWarnings("unused")
    @CommandLine.Mixin
    private ReplicationOptions replicationOptions = ReplicationOptions.builder().build();

    @Override
    protected Flow flow(StepBuilderFactory stepBuilderFactory) {
        if (replicationOptions.isVerify()) {
            return flow("replication-verification-flow").start(replicationFlow(stepBuilderFactory)).next(verificationFlow(stepBuilderFactory)).build();
        }
        return replicationFlow(stepBuilderFactory);
    }

    private Flow replicationFlow(StepBuilderFactory stepBuilderFactory) {
        switch (replicationOptions.getMode()) {
            case LIVE:
                SimpleFlow notificationFlow = flow("notification-flow").start(notificationStep(stepBuilderFactory).build()).build();
                SimpleFlow scanFlow = flow("scan-flow").start(scanStep(stepBuilderFactory)).build();
                return flow("live-flow").split(new SimpleAsyncTaskExecutor()).add(notificationFlow, scanFlow).build();
            case LIVEONLY:
                return flow("live-only-flow").start(notificationStep(stepBuilderFactory).build()).build();
            default:
                return flow("snapshot-flow").start(scanStep(stepBuilderFactory)).build();
        }
    }

    private TaskletStep scanStep(StepBuilderFactory stepBuilderFactory) {
        StepBuilder stepBuilder = stepBuilderFactory.get("scan-replication-step");
        RiotStepBuilder<T, T> scanStep = riotStep(stepBuilder, "Scanning");
        scanStep.initialMax(readerOptions.initialMaxSupplier(getRedisOptions()));
        return scanStep.reader(reader(getRedisOptions())).writer(writer(targetRedisOptions)).build().build();
    }

    @SuppressWarnings("rawtypes")
    private FlushingStepBuilder<T, T> notificationStep(StepBuilderFactory stepBuilderFactory) {
        StepBuilder stepBuilder = stepBuilderFactory.get("live-replication-step");
        RiotStepBuilder<T, T> notificationStep = riotStep(stepBuilder, "Listening");
        ItemReader<T> liveReader = liveReader(getRedisOptions());
        if (liveReader instanceof AbstractItemStreamItemReader) {
            ((AbstractItemStreamItemReader) liveReader).setName("Live" + ClassUtils.getShortName(liveReader.getClass()));
        }
        return configure(notificationStep.reader(liveReader).writer(writer(targetRedisOptions)).build());
    }

    protected abstract ItemReader<T> reader(RedisOptions redisOptions);

    protected abstract PollableItemReader<T> liveReader(RedisOptions redisOptions);

    protected abstract ItemWriter<T> writer(RedisOptions redisOptions);

    private Flow verificationFlow(StepBuilderFactory stepBuilderFactory) {
        KeyValueItemReader<String, DataStructure<String>> sourceReader = dataStructureReader();
        log.info("Creating key comparator with TTL tolerance of {} seconds", replicationOptions.getTtlTolerance());
        DataStructureValueReader<String, String> targetValueReader = targetDataStructureValueReader();
        Duration ttlToleranceDuration = Duration.ofSeconds(replicationOptions.getTtlTolerance());
        KeyComparisonItemWriter<String> writer = new KeyComparisonItemWriter<>(targetValueReader, ttlToleranceDuration);
        StepBuilder verificationStepBuilder = stepBuilderFactory.get("verification-step");
        RiotStepBuilder<DataStructure<String>, DataStructure<String>> stepBuilder = riotStep(verificationStepBuilder, "Verifying");
        SimpleStepBuilder<DataStructure<String>, DataStructure<String>> step = stepBuilder.reader(sourceReader).writer(writer).extraMessage(() -> extraMessage(writer.getResults())).build();
        step.listener(new VerificationStepExecutionListener(writer));
        TaskletStep verificationStep = step.build();
        return flow("verification-flow").start(verificationStep).build();
    }

    private String extraMessage(KeyComparisonResults<String> results) {
        return " " + String.format("OK:%s", results.getOk()) + " " + String.format("V:%s >:%s <:%s T:%s", results.getValue(), results.getLeft(), results.getRight(), results.getTtl());
    }

    protected KeyValueItemReader<String, DataStructure<String>> dataStructureReader() {
        RedisOptions redisOptions = getRedisOptions();
        if (redisOptions.isCluster()) {
            return readerOptions.configure(DataStructureItemReader.client(redisOptions.redisClusterClient()).poolConfig(redisOptions.poolConfig())).build();
        }
        return readerOptions.configure(DataStructureItemReader.client(redisOptions.redisClient()).poolConfig(redisOptions.poolConfig())).build();
    }

    protected DataStructureValueReader<String, String> targetDataStructureValueReader() {
        if (targetRedisOptions.isCluster()) {
            return DataStructureValueReader.client(targetRedisOptions.redisClusterClient()).poolConfig(targetRedisOptions.poolConfig()).build();
        }
        return DataStructureValueReader.client(targetRedisOptions.redisClient()).poolConfig(targetRedisOptions.poolConfig()).build();
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    protected <B extends KeyValueItemReader.LiveKeyValueItemReaderBuilder> B configure(B builder) {
        log.info("Configuring live reader with {}, queueCapacity={}", readerOptions, replicationOptions.getNotificationQueueCapacity());
        return (B) readerOptions.configure(builder.keyPattern(readerOptions.getScanMatch()).queueCapacity(replicationOptions.getNotificationQueueCapacity()).database(getRedisOptions().uris().get(0).getDatabase()).flushingInterval(flushingTransferOptions.getFlushIntervalDuration()).idleTimeout(flushingTransferOptions.getIdleTimeoutDuration()));
    }

    @SuppressWarnings("rawtypes")
    protected <B extends KeyValueItemReader.KeyValueItemReaderBuilder> B configure(B builder) {
        log.info("Configuring reader with {}", readerOptions);
        return readerOptions.configure(builder);
    }

}
