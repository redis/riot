package com.redislabs.riot.redis;

import com.redislabs.riot.AbstractFlushingTransferCommand;
import com.redislabs.riot.RedisOptions;
import com.redislabs.riot.RedisReaderOptions;
import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.pubsub.StatefulRedisClusterPubSubConnection;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.batch.core.job.flow.support.SimpleFlow;
import org.springframework.batch.core.step.builder.AbstractTaskletStepBuilder;
import org.springframework.batch.core.step.builder.SimpleStepBuilder;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.redis.*;
import org.springframework.batch.item.redis.support.*;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.util.ClassUtils;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.time.Duration;

@Command(name = "replicate", description = "Replicate a source Redis database to a target Redis database")
public class ReplicateCommand extends AbstractFlushingTransferCommand<KeyValue<String, byte[]>, KeyValue<String, byte[]>> {

    @CommandLine.ArgGroup(exclusive = false, heading = "Target Redis connection options%n")
    private RedisOptions targetRedis = new RedisOptions();
    @CommandLine.ArgGroup(exclusive = false, heading = "Source Redis reader options%n")
    private RedisReaderOptions options = RedisReaderOptions.builder().build();
    @Option(names = "--live", description = "Enable live replication.")
    private boolean live;
    @Option(names = "--event-queue", description = "Capacity of the keyspace notification event queue (default: ${DEFAULT-VALUE}).", paramLabel = "<size>")
    private int notificationQueueCapacity = KeyValueItemReaderBuilder.DEFAULT_NOTIFICATION_QUEUE_CAPACITY;
    @Option(names = "--no-verify", description = "Verify target against source dataset after replication. True by default.", negatable = true)
    private boolean verify = true;
    @Option(names = "--ttl-tolerance", description = "Max TTL difference to use for dataset verification (default: ${DEFAULT-VALUE}).", paramLabel = "<sec>")
    private long ttlTolerance = KeyComparisonItemWriter.KeyComparisonItemWriterBuilder.DEFAULT_TTL_TOLERANCE.getSeconds();

    private AbstractRedisClient targetClient;
    private GenericObjectPool<? extends StatefulConnection<String, String>> targetPool;
    private StatefulConnection<String, String> targetConnection;
    private StatefulRedisPubSubConnection<String, String> pubSubConnection;

    @Override
    public void afterPropertiesSet() throws Exception {
        this.targetClient = client(targetRedis);
        this.targetPool = pool(targetRedis, targetClient);
        this.targetConnection = connection(targetClient);
        super.afterPropertiesSet();
        if (live) {
            this.pubSubConnection = pubSubConnection(client);
        }
    }

    private StatefulRedisPubSubConnection<String, String> pubSubConnection(AbstractRedisClient client) {
        if (client instanceof RedisClusterClient) {
            return ((RedisClusterClient) client).connectPubSub();
        }
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
        String name = "Scanning";
        FlowBuilder<SimpleFlow> flow = flowBuilder(name).start(step(name, sourceKeyDumpReader(), null, targetKeyDumpWriter()).build());
        if (live) {
            String liveName = "Listening";
            AbstractKeyDumpItemReader<String, String, ?> liveReader = liveReader();
            liveReader.setName("Live" + ClassUtils.getShortName(liveReader.getClass()));
            SimpleFlow liveFlow = flowBuilder(liveName).start(step(liveName, liveReader, null, targetKeyDumpWriter(), null).build()).build();
            flow = flowBuilder(liveName).split(new SimpleAsyncTaskExecutor()).add(flow.build(), liveFlow);
        }
        if (verify) {
            KeyComparisonItemWriter<String> writer = comparisonWriter();
            AbstractTaskletStepBuilder<SimpleStepBuilder<DataStructure<String>, DataStructure<String>>> verificationStep = step("Verifying", sourceDataStructureReader(), null, writer, () -> message(writer));
            flow = flowBuilder("Replication+Verification").start(flow.build()).next(verificationStep.build());
        }
        return flow.build();
    }

    private String message(KeyComparisonItemWriter<String> writer) {
        int v = writer.getDiffs().get(KeyComparisonItemWriter.DiffType.VALUE).size();
        int l = writer.getDiffs().get(KeyComparisonItemWriter.DiffType.LEFT_ONLY).size();
        int r = writer.getDiffs().get(KeyComparisonItemWriter.DiffType.LEFT_ONLY).size();
        int t = writer.getDiffs().get(KeyComparisonItemWriter.DiffType.TTL).size();
        return String.format(" OK:%s V:%s >:%s <:%s T:%s", writer.getOkCount(), v, l, r, t);
    }

    private KeyComparisonItemWriter<String> comparisonWriter() {
        Duration ttlToleranceDuration = Duration.ofSeconds(ttlTolerance);
        if (targetRedis.isCluster()) {
            return KeyComparisonItemWriter.builder(RedisClusterDataStructureItemReader.builder((GenericObjectPool<StatefulRedisClusterConnection<String, String>>) targetPool, (StatefulRedisClusterConnection<String, String>) targetConnection).commandTimeout(getTargetCommandTimeout()).build()).ttlTolerance(ttlToleranceDuration).build();
        }
        return KeyComparisonItemWriter.builder(RedisDataStructureItemReader.builder((GenericObjectPool<StatefulRedisConnection<String, String>>) targetPool, (StatefulRedisConnection<String, String>) targetConnection).commandTimeout(getTargetCommandTimeout()).build()).ttlTolerance(ttlToleranceDuration).build();
    }

    private ItemReader<KeyValue<String, byte[]>> sourceKeyDumpReader() {
        if (isCluster()) {
            return configureScanKeyValueItemReaderBuilder(RedisClusterKeyDumpItemReader.builder((GenericObjectPool<StatefulRedisClusterConnection<String, String>>) pool, (StatefulRedisClusterConnection<String, String>) connection)).build();
        }
        return configureScanKeyValueItemReaderBuilder(RedisKeyDumpItemReader.builder((GenericObjectPool<StatefulRedisConnection<String, String>>) pool, (StatefulRedisConnection<String, String>) connection)).build();
    }

    private ItemReader<DataStructure<String>> sourceDataStructureReader() {
        if (isCluster()) {
            return configureScanKeyValueItemReaderBuilder(RedisClusterDataStructureItemReader.builder((GenericObjectPool<StatefulRedisClusterConnection<String, String>>) pool, (StatefulRedisClusterConnection<String, String>) connection)).build();
        }
        return configureScanKeyValueItemReaderBuilder(RedisDataStructureItemReader.builder((GenericObjectPool<StatefulRedisConnection<String, String>>) pool, (StatefulRedisConnection<String, String>) connection)).build();
    }


    private AbstractKeyDumpItemReader<String, String, ?> liveReader() {
        if (isCluster()) {
            return configureNotificationKeyValueItemReaderBuilder(RedisClusterKeyDumpItemReader.builder((GenericObjectPool<StatefulRedisClusterConnection<String, String>>) pool, (StatefulRedisClusterPubSubConnection<String, String>) pubSubConnection)).build();
        }
        return configureNotificationKeyValueItemReaderBuilder(RedisKeyDumpItemReader.builder((GenericObjectPool<StatefulRedisConnection<String, String>>) pool, pubSubConnection)).build();
    }

    private <B extends ScanKeyValueItemReaderBuilder<B>> B configureScanKeyValueItemReaderBuilder(B builder) {
        return configureSourceKeyValueItemReaderBuilder(builder.scanCount(options.getScanCount()).sampleSize(options.getSampleSize()));
    }

    private <B extends NotificationKeyValueItemReaderBuilder<B>> B configureNotificationKeyValueItemReaderBuilder(B builder) {
        return configureSourceKeyValueItemReaderBuilder(builder.notificationQueueCapacity(notificationQueueCapacity).database(getRedisURI().getDatabase()));
    }

    private <B extends KeyValueItemReaderBuilder<B>> B configureSourceKeyValueItemReaderBuilder(B builder) {
        return builder.keyPattern(options.getScanMatch()).threadCount(options.getThreads()).chunkSize(options.getBatchSize()).commandTimeout(getCommandTimeout()).queueCapacity(options.getQueueCapacity());
    }

    private ItemWriter<KeyValue<String, byte[]>> targetKeyDumpWriter() {
        if (targetRedis.isCluster()) {
            return RedisClusterKeyDumpItemWriter.builder((GenericObjectPool<StatefulRedisClusterConnection<String, String>>) targetPool).replace(true).commandTimeout(getTargetCommandTimeout()).build();
        }
        return RedisKeyDumpItemWriter.builder((GenericObjectPool<StatefulRedisConnection<String, String>>) targetPool).replace(true).commandTimeout(getTargetCommandTimeout()).build();
    }

    private Duration getTargetCommandTimeout() {
        return targetRedis.uris().get(0).getTimeout();
    }

}
