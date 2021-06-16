package com.redislabs.riot.redis;

import com.redislabs.riot.RedisOptions;
import com.redislabs.riot.RiotStepBuilder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.batch.core.job.flow.support.SimpleFlow;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.core.step.tasklet.TaskletStep;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.redis.support.FlushingStepBuilder;
import org.springframework.batch.item.redis.support.KeyValue;
import org.springframework.batch.item.redis.support.KeyValueItemReader;
import org.springframework.batch.item.redis.support.PollableItemReader;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import picocli.CommandLine;

@Data
@EqualsAndHashCode(callSuper = true)
@Slf4j
public abstract class AbstractReplicateCommand<T extends KeyValue<?>> extends AbstractTargetCommand {

    @SuppressWarnings("unused")
    @CommandLine.Mixin
    private ReplicationOptions replicationOptions = new ReplicationOptions();

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
        initialMax(scanStep);
        return scanStep.reader(reader(getRedisOptions())).writer(writer(targetRedisOptions)).build().build();
    }

    private FlushingStepBuilder<T, T> notificationStep(StepBuilderFactory stepBuilderFactory) {
        StepBuilder stepBuilder = stepBuilderFactory.get("live-replication-step");
        RiotStepBuilder<T, T> notificationStep = riotStep(stepBuilder, "Listening");
        return configure(notificationStep.reader(liveReader(getRedisOptions())).writer(writer(targetRedisOptions)).build());
    }

    protected abstract ItemReader<T> reader(RedisOptions redisOptions);

    protected abstract PollableItemReader<T> liveReader(RedisOptions redisOptions);

    protected abstract ItemWriter<T> writer(RedisOptions redisOptions);

    @SuppressWarnings({"rawtypes", "unchecked"})
    protected <B extends KeyValueItemReader.LiveKeyValueItemReaderBuilder> B configure(B builder) {
        log.debug("Configuring live reader with {}, queueCapacity={}", readerOptions, replicationOptions.getNotificationQueueCapacity());
        return (B) readerOptions.configure(builder.keyPattern(readerOptions.getScanMatch()).queueCapacity(replicationOptions.getNotificationQueueCapacity()).database(getRedisOptions().uris().get(0).getDatabase()).flushingInterval(flushingTransferOptions.getFlushIntervalDuration()).idleTimeout(flushingTransferOptions.getIdleTimeoutDuration()));
    }

}
