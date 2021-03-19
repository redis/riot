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
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.batch.core.job.flow.support.SimpleFlow;
import org.springframework.batch.core.listener.StepExecutionListenerSupport;
import org.springframework.batch.core.step.builder.SimpleStepBuilder;
import org.springframework.batch.core.step.tasklet.TaskletStep;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.redis.DataStructureItemReader;
import org.springframework.batch.item.redis.DataStructureItemWriter;
import org.springframework.batch.item.redis.KeyDumpItemReader;
import org.springframework.batch.item.redis.KeyDumpItemWriter;
import org.springframework.batch.item.redis.support.*;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.util.ClassUtils;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.time.Duration;
import java.util.List;

@Slf4j
@Command(name = "replicate", description = "Replicate a source Redis database to a target Redis database")
public class ReplicateCommand extends AbstractTransferCommand<KeyValue<String, byte[]>, KeyValue<String, byte[]>> {

    enum ReplicationMode {
        SNAPSHOT, LIVE, LIVEONLY
    }

    enum ReplicationStrategy {
        DUMP, VALUE
    }

    @CommandLine.ArgGroup(exclusive = false, heading = "Target Redis connection options%n")
    private RedisOptions targetRedisOptions = RedisOptions.builder().build();
    @CommandLine.ArgGroup(exclusive = false, heading = "Source Redis reader options%n")
    private RedisReaderOptions readerOptions = RedisReaderOptions.builder().build();
    @SuppressWarnings("unused")
    @Option(names = "--mode", description = "Replication mode: ${COMPLETION-CANDIDATES} (default: ${DEFAULT-VALUE}).", paramLabel = "<name>")
    private ReplicationMode mode = ReplicationMode.SNAPSHOT;
    @Option(names = "--strategy", description = "Data structure read/write strategy: ${COMPLETION-CANDIDATES} (default: ${DEFAULT-VALUE}).", paramLabel = "<name>")
    private ReplicationStrategy strategy = ReplicationStrategy.DUMP;
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
        if (mode == ReplicationMode.LIVE || mode == ReplicationMode.LIVEONLY) {
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
            case LIVEONLY:
                return flow("live-only-flow").start(notificationStep()).build();
            default:
                return flow("snapshot-flow").start(scanStep()).build();
        }
    }

    private Flow verificationFlow() {
        KeyComparisonItemWriter<String, String> writer = comparisonWriter();
        StepBuilder<DataStructure<String>, DataStructure<String>> stepBuilder = stepBuilder("verification-step", "Verifying");
        SimpleStepBuilder<DataStructure<String>, DataStructure<String>> step = stepBuilder.reader(sourceDataStructureReader()).writer(writer).extraMessage(() -> extraMessage(writer.getResults())).build();
        step.listener(new VerificationStepExecutionListener(writer));
        TaskletStep verificationStep = step.build();
        return flow("verification-flow").start(verificationStep).build();
    }

    private static class VerificationStepExecutionListener extends StepExecutionListenerSupport {

        private final KeyComparisonItemWriter<String, String> writer;

        public VerificationStepExecutionListener(KeyComparisonItemWriter<String, String> writer) {
            this.writer = writer;
        }

        @Override
        public ExitStatus afterStep(StepExecution stepExecution) {
            if (writer.getResults().isOk()) {
                return super.afterStep(stepExecution);
            }
            log.warn("Verification failed");
            KeyComparisonResults<String> results = writer.getResults();
            printDiffs(results.getLeft(), "missing keys");
            printDiffs(results.getRight(), "extraneous keys");
            printDiffs(results.getValue(), "mismatched values");
            printDiffs(results.getTtl(), "mismatched TTLs");
            return new ExitStatus(ExitStatus.FAILED.getExitCode(), "Verification failed");
        }

        private void printDiffs(List<String> diffs, String messagePreamble) {
            if (diffs.isEmpty()) {
                return;
            }
            log.info("{} " + messagePreamble + ": {}", diffs.size(), diffs.subList(0, Math.min(10, diffs.size())));
        }
    }

    private String extraMessage(KeyComparisonResults<String> results) {
        return " " + String.format("OK:%s", results.getOk()) + " " + String.format("V:%s >:%s <:%s T:%s", results.getValue(), results.getLeft(), results.getRight(), results.getTtl());
    }

    private TaskletStep scanStep() {
        if (strategy == ReplicationStrategy.VALUE) {
            StepBuilder<DataStructure<String>, DataStructure<String>> dataStructureReplicationStep = stepBuilder("scan-replication-step", "Scanning");
            return dataStructureReplicationStep.reader(sourceDataStructureReader()).writer(targetDataStructureWriter()).build().build();
        }
        StepBuilder<KeyValue<String, byte[]>, KeyValue<String, byte[]>> replicationStep = stepBuilder("scan-replication-step", "Scanning");
        return replicationStep.reader(sourceKeyDumpReader()).writer(targetKeyDumpWriter()).build().build();
    }

    private TaskletStep notificationStep() {
        if (strategy == ReplicationStrategy.VALUE) {
            StepBuilder<DataStructure<String>, DataStructure<String>> liveDataStructureReplicationStep = stepBuilder("live-replication-step", "Listening");
            DataStructureItemReader<String, String> liveDataStructureReader = liveDataStructureReader();
            liveDataStructureReader.setName("Live" + ClassUtils.getShortName(liveDataStructureReader.getClass()));
            log.info("Configuring live transfer with {}", flushingOptions);
            return flushingOptions.configure(liveDataStructureReplicationStep.reader(liveDataStructureReader).writer(targetDataStructureWriter()).build()).build();
        }
        StepBuilder<KeyValue<String, byte[]>, KeyValue<String, byte[]>> liveReplicationStep = stepBuilder("live-replication-step", "Listening");
        KeyDumpItemReader<String, String> liveReader = liveKeyDumpReader();
        liveReader.setName("Live" + ClassUtils.getShortName(liveReader.getClass()));
        log.info("Configuring live transfer with {}", flushingOptions);
        return flushingOptions.configure(liveReplicationStep.reader(liveReader).writer(targetKeyDumpWriter()).build()).build();
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
    private KeyDumpItemReader<String, String> liveKeyDumpReader() {
        if (isCluster()) {
            return configureLiveReader(KeyDumpItemReader.builder((GenericObjectPool<StatefulRedisClusterConnection<String, String>>) pool, (StatefulRedisClusterPubSubConnection<String, String>) pubSubConnection)).build();
        }
        return configureLiveReader(KeyDumpItemReader.builder((GenericObjectPool<StatefulRedisConnection<String, String>>) pool, pubSubConnection)).build();
    }

    @SuppressWarnings("unchecked")
    private DataStructureItemReader<String, String> liveDataStructureReader() {
        if (isCluster()) {
            return configureLiveReader(DataStructureItemReader.builder((GenericObjectPool<StatefulRedisClusterConnection<String, String>>) pool, (StatefulRedisClusterPubSubConnection<String, String>) pubSubConnection)).build();
        }
        return configureLiveReader(DataStructureItemReader.builder((GenericObjectPool<StatefulRedisConnection<String, String>>) pool, pubSubConnection)).build();
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
            return targetRedisOptions.configureCommandTimeout(KeyDumpItemWriter.clusterBuilder((GenericObjectPool<StatefulRedisClusterConnection<String, String>>) targetPool)).replace(true).build();
        }
        log.info("Creating Redis key dump writer");
        return targetRedisOptions.configureCommandTimeout(KeyDumpItemWriter.builder((GenericObjectPool<StatefulRedisConnection<String, String>>) targetPool)).replace(true).build();
    }

    @SuppressWarnings("unchecked")
    private ItemWriter<DataStructure<String>> targetDataStructureWriter() {
        if (targetRedisOptions.isCluster()) {
            log.info("Creating Redis cluster data structure writer");
            return targetRedisOptions.configureCommandTimeout(DataStructureItemWriter.clusterBuilder((GenericObjectPool<StatefulRedisClusterConnection<String, String>>) targetPool)).build();
        }
        log.info("Creating Redis data structure writer");
        return targetRedisOptions.configureCommandTimeout(DataStructureItemWriter.builder((GenericObjectPool<StatefulRedisConnection<String, String>>) targetPool)).build();
    }

}
