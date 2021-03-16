package com.redislabs.riot.redis;

import com.redislabs.riot.*;
import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.pubsub.StatefulRedisClusterPubSubConnection;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.batch.core.job.flow.support.SimpleFlow;
import org.springframework.batch.core.step.tasklet.TaskletStep;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.redis.DataStructureItemReader;
import org.springframework.batch.item.redis.KeyDumpItemReader;
import org.springframework.batch.item.redis.KeyDumpItemWriter;
import org.springframework.batch.item.redis.support.*;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.util.ClassUtils;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.time.Duration;

@Slf4j
@Command(name = "replicate", description = "Replicate a source Redis database to a target Redis database")
public class ReplicateCommand extends AbstractTransferCommand<KeyValue<String, byte[]>, KeyValue<String, byte[]>> {

    enum ReplicationMode {
        SNAPSHOT, LIVE, LIVE_ONLY
    }

    @CommandLine.ArgGroup(exclusive = false, heading = "Target Redis connection options%n")
    private RedisOptions targetRedisOptions = RedisOptions.builder().build();
    @CommandLine.ArgGroup(exclusive = false, heading = "Source Redis reader options%n")
    private RedisReaderOptions readerOptions = RedisReaderOptions.builder().build();
    @SuppressWarnings("unused")
    @Option(names = "--mode", description = "Replication mode: SNAPSHOT (scan only), LIVE (scan+notifications), LIVE_ONLY (notifications). Default: ${DEFAULT-VALUE}.")
    private ReplicationMode mode = ReplicationMode.SNAPSHOT;
    @CommandLine.Mixin
    private FlushingTransferOptions flushingOptions = FlushingTransferOptions.builder().build();
    @Option(names = "--event-queue", description = "Capacity of the keyspace notification event queue (default: ${DEFAULT-VALUE}).", paramLabel = "<size>")
    private int notificationQueueCapacity = LiveKeyValueItemReaderBuilder.DEFAULT_NOTIFICATION_QUEUE_CAPACITY;
    @Option(names = "--no-verify", description = "Verify target against source dataset after replication. True by default.", negatable = true)
    private boolean verify = true;
    @Option(names = "--ttl-tolerance", description = "Max TTL difference to use for dataset verification (default: ${DEFAULT-VALUE}).", paramLabel = "<sec>")
    private long ttlTolerance = 1;

    private AbstractRedisClient targetClient;
    private GenericObjectPool<? extends StatefulConnection<String, String>> targetPool;
    private StatefulConnection<String, String> targetConnection;
    private StatefulRedisPubSubConnection<String, String> pubSubConnection;

    @Override
    public void afterPropertiesSet() throws Exception {
        this.targetClient = targetRedisOptions.client();
        this.targetPool = pool(targetRedisOptions, targetClient);
        this.targetConnection = connection(targetClient);
        super.afterPropertiesSet();
        if (mode == ReplicationMode.LIVE || mode == ReplicationMode.LIVE_ONLY) {
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
        if (verify) {
            return flow("replication-verification-flow").start(replicationFlow()).next(verificationFlow()).build();
        }
        return replicationFlow();
    }

    private Flow replicationFlow() {
        switch (mode) {
            case LIVE:
                SimpleFlow notificationFlow = flow("notification-flow").start(notificationStep()).build();
                SimpleFlow scanFlow = flow("scan-flow").start(scanStep()).build();
                return flow("live-flow").split(new SimpleAsyncTaskExecutor()).add(notificationFlow, scanFlow).build();
            case LIVE_ONLY:
                return flow("live-only-flow").start(notificationStep()).build();
            default:
                return flow("snapshot-flow").start(scanStep()).build();
        }
    }

    private Flow verificationFlow() {
        KeyComparisonItemWriter<String, String> writer = comparisonWriter();
        StepBuilder<DataStructure<String>, DataStructure<String>> verifyStep = stepBuilder("verification-step", "Verifying");
        TaskletStep verificationStep = verifyStep.reader(sourceDataStructureReader()).writer(writer).extraMessage(() -> message(writer)).build().build();
        return flow("verification-flow").start(verificationStep).build();
    }

    private TaskletStep scanStep() {
        StepBuilder<KeyValue<String, byte[]>, KeyValue<String, byte[]>> replicationStep = stepBuilder("scan-replication-step", "Scanning");
        return replicationStep.reader(sourceKeyDumpReader()).writer(targetKeyDumpWriter()).build().build();
    }

    private TaskletStep notificationStep() {
        StepBuilder<KeyValue<String, byte[]>, KeyValue<String, byte[]>> liveReplicationStep = stepBuilder("live-replication-step", "Listening");
        KeyDumpItemReader<String, String> liveReader = liveReader();
        liveReader.setName("Live" + ClassUtils.getShortName(liveReader.getClass()));
        log.info("Configuring live transfer with {}", flushingOptions);
        return flushingOptions.configure(liveReplicationStep.reader(liveReader).writer(targetKeyDumpWriter()).build()).build();
    }

    private String message(KeyComparisonItemWriter<String, String> writer) {
        int v = writer.getDiffs().get(KeyComparisonItemWriter.DiffType.VALUE).size();
        int l = writer.getDiffs().get(KeyComparisonItemWriter.DiffType.LEFT_ONLY).size();
        int r = writer.getDiffs().get(KeyComparisonItemWriter.DiffType.LEFT_ONLY).size();
        int t = writer.getDiffs().get(KeyComparisonItemWriter.DiffType.TTL).size();
        return String.format(" OK:%s V:%s >:%s <:%s T:%s", writer.getOkCount(), v, l, r, t);
    }

    @SuppressWarnings("unchecked")
    private KeyComparisonItemWriter<String, String> comparisonWriter() {
        log.info("Creating key comparator with TTL tolerance of {} seconds", ttlTolerance);
        Duration ttlToleranceDuration = Duration.ofSeconds(ttlTolerance);
        if (targetRedisOptions.isCluster()) {
            DataStructureItemReader<String, String> targetReader = configureScanReader(DataStructureItemReader.builder((GenericObjectPool<StatefulRedisClusterConnection<String, String>>) targetPool, (StatefulRedisClusterConnection<String, String>) targetConnection)).build();
            return new KeyComparisonItemWriter<>(targetReader, ttlToleranceDuration);
        }
        DataStructureItemReader<String, String> targetReader = configureScanReader(DataStructureItemReader.builder((GenericObjectPool<StatefulRedisConnection<String, String>>) targetPool, (StatefulRedisConnection<String, String>) targetConnection)).build();
        return new KeyComparisonItemWriter<>(targetReader, ttlToleranceDuration);
    }

    @SuppressWarnings("unchecked")
    private ItemReader<KeyValue<String, byte[]>> sourceKeyDumpReader() {
        if (isCluster()) {
            return configureScanReader(KeyDumpItemReader.builder((GenericObjectPool<StatefulRedisClusterConnection<String, String>>) pool, (StatefulRedisClusterConnection<String, String>) connection)).build();
        }
        return configureScanReader(KeyDumpItemReader.builder((GenericObjectPool<StatefulRedisConnection<String, String>>) pool, (StatefulRedisConnection<String, String>) connection)).build();
    }

    @SuppressWarnings("unchecked")
    private ItemReader<DataStructure<String>> sourceDataStructureReader() {
        if (isCluster()) {
            return configureScanReader(DataStructureItemReader.builder((GenericObjectPool<StatefulRedisClusterConnection<String, String>>) pool, (StatefulRedisClusterConnection<String, String>) connection)).build();
        }
        return configureScanReader(DataStructureItemReader.builder((GenericObjectPool<StatefulRedisConnection<String, String>>) pool, (StatefulRedisConnection<String, String>) connection)).build();
    }

    @SuppressWarnings("unchecked")
    private KeyDumpItemReader<String, String> liveReader() {
        if (isCluster()) {
            return configureLiveReader(KeyDumpItemReader.builder((GenericObjectPool<StatefulRedisClusterConnection<String, String>>) pool, (StatefulRedisClusterPubSubConnection<String, String>) pubSubConnection)).build();
        }
        return configureLiveReader(KeyDumpItemReader.builder((GenericObjectPool<StatefulRedisConnection<String, String>>) pool, pubSubConnection)).build();
    }

    private <B extends ScanKeyValueItemReaderBuilder<?>> B configureScanReader(B builder) {
        log.info("Configuring scan reader with {}", readerOptions);
        configureReader(builder.scanMatch(readerOptions.getScanMatch()).scanCount(readerOptions.getScanCount()).sampleSize(readerOptions.getSampleSize()));
        return builder;
    }

    private <B extends LiveKeyValueItemReaderBuilder<?>> B configureLiveReader(B builder) {
        log.info("Configuring live reader with {}, {}, queueCapacity={}", readerOptions, flushingOptions, notificationQueueCapacity);
        configureReader(builder.keyPattern(readerOptions.getScanMatch()).notificationQueueCapacity(notificationQueueCapacity).database(getRedisURI().getDatabase()).flushingInterval(flushingOptions.getFlushIntervalDuration()).idleTimeout(flushingOptions.getIdleTimeoutDuration()));
        return builder;
    }

    private <B extends AbstractKeyValueItemReader.AbstractKeyValueItemReaderBuilder<?, B>> void configureReader(B builder) {
        configureCommandTimeoutBuilder(builder.threadCount(readerOptions.getThreads()).chunkSize(readerOptions.getBatchSize()).queueCapacity(readerOptions.getQueueCapacity()));
    }

    @SuppressWarnings("unchecked")
    private ItemWriter<KeyValue<String, byte[]>> targetKeyDumpWriter() {
        if (targetRedisOptions.isCluster()) {
            log.info("Creating Redis cluster key dump writer");
            return configure(KeyDumpItemWriter.clusterBuilder((GenericObjectPool<StatefulRedisClusterConnection<String, String>>) targetPool)).build();
        }
        log.info("Creating Redis key dump writer");
        return configure(KeyDumpItemWriter.builder((GenericObjectPool<StatefulRedisConnection<String, String>>) targetPool)).build();
    }

    private KeyDumpItemWriter.KeyDumpItemWriterBuilder<String, String> configure(KeyDumpItemWriter.KeyDumpItemWriterBuilder<String, String> builder) {
        Duration commandTimeout = targetRedisOptions.uris().get(0).getTimeout();
        log.info("Setting key dump writer command timeout to {}", commandTimeout);
        return builder.replace(true).commandTimeout(commandTimeout);
    }

}
