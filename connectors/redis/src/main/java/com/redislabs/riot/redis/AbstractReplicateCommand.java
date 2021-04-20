package com.redislabs.riot.redis;

import com.redislabs.riot.AbstractFlushingTransferCommand;
import com.redislabs.riot.RedisOptions;
import com.redislabs.riot.RedisReaderOptions;
import com.redislabs.riot.StepBuilder;
import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.pubsub.StatefulRedisClusterPubSubConnection;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.batch.core.job.flow.support.SimpleFlow;
import org.springframework.batch.core.step.builder.SimpleStepBuilder;
import org.springframework.batch.core.step.tasklet.TaskletStep;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.redis.RedisClusterDataStructureItemReader;
import org.springframework.batch.item.redis.RedisDataStructureItemReader;
import org.springframework.batch.item.redis.support.*;
import org.springframework.batch.item.support.AbstractItemStreamItemReader;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.util.ClassUtils;
import picocli.CommandLine;

import java.time.Duration;
import java.util.function.Supplier;

@Slf4j
public abstract class AbstractReplicateCommand<T extends KeyValue<String, ?>> extends AbstractFlushingTransferCommand {

    @Getter
    @Setter
    @CommandLine.ArgGroup(exclusive = false, heading = "Target Redis connection options%n")
    private RedisOptions targetRedisOptions = RedisOptions.builder().build();
    @Getter
    @Setter
    @CommandLine.ArgGroup(exclusive = false, heading = "Source Redis reader options%n")
    protected RedisReaderOptions readerOptions = RedisReaderOptions.builder().build();
    @Getter
    @Setter
    @SuppressWarnings("unused")
    @CommandLine.Mixin
    private ReplicationOptions replicationOptions = ReplicationOptions.builder().build();

    private AbstractRedisClient targetClient;
    private GenericObjectPool<? extends StatefulConnection<String, String>> targetPool;
    private StatefulConnection<String, String> targetConnection;
    private StatefulRedisPubSubConnection<String, String> pubSubConnection;

    @Override
    public void afterPropertiesSet() throws Exception {
        this.targetClient = targetRedisOptions.client();
        this.targetPool = pool(targetRedisOptions, targetClient);
        this.targetConnection = RedisOptions.connection(targetClient);
        super.afterPropertiesSet();
        if (replicationOptions.getMode() == ReplicationMode.LIVE || replicationOptions.getMode() == ReplicationMode.LIVEONLY) {
            this.pubSubConnection = pubSubConnection(client);
        }
    }

    private StatefulRedisPubSubConnection<String, String> pubSubConnection(AbstractRedisClient client) {
        if (client instanceof RedisClusterClient) {
            log.info("Establishing Redis cluster pub/sub connection");
            return ((RedisClusterClient) client).connectPubSub();
        }
        log.info("Establishing Redis pub/sub connection");
        return ((RedisClient) client).connectPubSub();
    }

    @Override
    public void shutdown() {
        if (pubSubConnection != null) {
            pubSubConnection.close();
        }
        super.shutdown();
        if (targetConnection != null) {
            targetConnection.close();
        }
        if (targetPool != null) {
            targetPool.close();
        }
        if (targetClient != null) {
            targetClient.shutdown();
            targetClient.getResources().shutdown();
        }
    }

    @Override
    protected Flow flow() {
        if (replicationOptions.isVerify()) {
            return flow("replication-verification-flow").start(replicationFlow()).next(verificationFlow()).build();
        }
        return replicationFlow();
    }

    private Flow replicationFlow() {
        switch (replicationOptions.getMode()) {
            case LIVE:
                SimpleFlow notificationFlow = flow("notification-flow").start(notificationStep().build()).build();
                SimpleFlow scanFlow = flow("scan-flow").start(scanStep()).build();
                return flow("live-flow").split(new SimpleAsyncTaskExecutor()).add(notificationFlow, scanFlow).build();
            case LIVEONLY:
                return flow("live-only-flow").start(notificationStep().build()).build();
            default:
                return flow("snapshot-flow").start(scanStep()).build();
        }
    }

    private TaskletStep scanStep() {
        StepBuilder<T, T> scanStep = stepBuilder("scan-replication-step", "Scanning");
        return scanStep.reader(reader(pool, connection)).writer(writer(targetPool, targetConnection)).build().build();
    }

    @SuppressWarnings("rawtypes")
    private FlushingStepBuilder<T, T> notificationStep() {
        StepBuilder<T, T> notificationStep = stepBuilder("live-replication-step", "Listening");
        ItemReader<T> liveReader = liveReader(pool, pubSubConnection);
        if (liveReader instanceof AbstractItemStreamItemReader) {
            ((AbstractItemStreamItemReader) liveReader).setName("Live" + ClassUtils.getShortName(liveReader.getClass()));
        }
        return configure(notificationStep.reader(liveReader).writer(writer(targetPool, targetConnection)).build());
    }

    @SuppressWarnings("unchecked")
    private ItemReader<T> reader(GenericObjectPool<? extends StatefulConnection<String, String>> pool, StatefulConnection<String, String> connection) {
        if (connection instanceof StatefulRedisClusterConnection) {
            return redisClusterReader((GenericObjectPool<StatefulRedisClusterConnection<String, String>>) pool, (StatefulRedisClusterConnection<String, String>) connection);
        }
        return redisReader((GenericObjectPool<StatefulRedisConnection<String, String>>) pool, (StatefulRedisConnection<String, String>) connection);
    }

    protected abstract ItemReader<T> redisReader(GenericObjectPool<StatefulRedisConnection<String, String>> pool, StatefulRedisConnection<String, String> connection);

    protected abstract ItemReader<T> redisClusterReader(GenericObjectPool<StatefulRedisClusterConnection<String, String>> pool, StatefulRedisClusterConnection<String, String> connection);

    @SuppressWarnings("unchecked")
    private ItemReader<T> liveReader(GenericObjectPool<? extends StatefulConnection<String, String>> pool, StatefulRedisPubSubConnection<String, String> pubSubConnection) {
        if (pubSubConnection instanceof StatefulRedisClusterPubSubConnection) {
            return liveRedisClusterReader((GenericObjectPool<StatefulRedisClusterConnection<String, String>>) pool, (StatefulRedisClusterPubSubConnection<String, String>) pubSubConnection);
        }
        return liveRedisReader((GenericObjectPool<StatefulRedisConnection<String, String>>) pool, pubSubConnection);
    }

    protected abstract ItemReader<T> liveRedisReader(GenericObjectPool<StatefulRedisConnection<String, String>> pool, StatefulRedisPubSubConnection<String, String> pubSubConnection);

    protected abstract ItemReader<T> liveRedisClusterReader(GenericObjectPool<StatefulRedisClusterConnection<String, String>> pool, StatefulRedisClusterPubSubConnection<String, String> pubSubConnection);

    @SuppressWarnings("unchecked")
    private ItemWriter<T> writer(GenericObjectPool<? extends StatefulConnection<String, String>> pool, StatefulConnection<String, String> connection) {
        if (connection instanceof StatefulRedisClusterConnection) {
            return redisClusterWriter((GenericObjectPool<StatefulRedisClusterConnection<String, String>>) pool, (StatefulRedisClusterConnection<String, String>) connection);
        }
        return redisWriter((GenericObjectPool<StatefulRedisConnection<String, String>>) pool, (StatefulRedisConnection<String, String>) connection);
    }

    protected abstract ItemWriter<T> redisWriter(GenericObjectPool<StatefulRedisConnection<String, String>> pool, StatefulRedisConnection<String, String> connection);

    protected abstract ItemWriter<T> redisClusterWriter(GenericObjectPool<StatefulRedisClusterConnection<String, String>> pool, StatefulRedisClusterConnection<String, String> connection);

    private Flow verificationFlow() {
        DataStructureItemReader<String, String, ?> sourceReader = dataStructureReader(pool, connection);
        log.info("Creating key comparator with TTL tolerance of {} seconds", replicationOptions.getTtlTolerance());
        DataStructureItemReader<String, String, ?> targetReader = dataStructureReader(targetPool, targetConnection);
        Duration ttlToleranceDuration = Duration.ofSeconds(replicationOptions.getTtlTolerance());
        KeyComparisonItemWriter<String, String> writer = new KeyComparisonItemWriter<>(targetReader, ttlToleranceDuration);
        StepBuilder<DataStructure<String>, DataStructure<String>> stepBuilder = stepBuilder("verification-step", "Verifying");
        SimpleStepBuilder<DataStructure<String>, DataStructure<String>> step = stepBuilder.reader(sourceReader).writer(writer).extraMessage(() -> extraMessage(writer.getResults())).build();
        step.listener(new VerificationStepExecutionListener(writer));
        TaskletStep verificationStep = step.build();
        return flow("verification-flow").start(verificationStep).build();
    }

    private String extraMessage(KeyComparisonResults<String> results) {
        return " " + String.format("OK:%s", results.getOk()) + " " + String.format("V:%s >:%s <:%s T:%s", results.getValue(), results.getLeft(), results.getRight(), results.getTtl());
    }

    @SuppressWarnings("unchecked")
    protected DataStructureItemReader<String, String, ?> dataStructureReader(GenericObjectPool<? extends StatefulConnection<String, String>> pool, StatefulConnection<String, String> connection) {
        if (connection instanceof StatefulRedisClusterConnection) {
            return readerOptions.configureScan(RedisClusterDataStructureItemReader.builder((GenericObjectPool<StatefulRedisClusterConnection<String, String>>) pool, (StatefulRedisClusterConnection<String, String>) connection)).build();
        }
        return readerOptions.configureScan(RedisDataStructureItemReader.builder((GenericObjectPool<StatefulRedisConnection<String, String>>) pool, (StatefulRedisConnection<String, String>) connection)).build();
    }

    protected <B extends AbstractLiveKeyValueItemReaderBuilder> B configureLiveReader(B builder) {
        log.info("Configuring live reader with {}, queueCapacity={}", readerOptions, replicationOptions.getNotificationQueueCapacity());
        return (B) readerOptions.configure(builder.keyPattern(readerOptions.getScanMatch()).notificationQueueCapacity(replicationOptions.getNotificationQueueCapacity()).database(getRedisURI().getDatabase()).flushingInterval(flushingTransferOptions.getFlushIntervalDuration()).idleTimeout(flushingTransferOptions.getIdleTimeoutDuration()));
    }

    @Override
    protected Supplier<Long> initialMax() {
        return initialMax(readerOptions.sizeEstimatorOptions());
    }
}
