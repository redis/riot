package com.redislabs.riot.redis;

import com.redislabs.riot.AbstractFlushingTransferCommand;
import com.redislabs.riot.RedisExportOptions;
import com.redislabs.riot.RedisOptions;
import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
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
import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Option;

import java.time.Duration;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

@Command(name = "replicate", description = "Replicate a source Redis database in a target Redis database")
public class ReplicateCommand extends AbstractFlushingTransferCommand<KeyValue<String, byte[]>, KeyValue<String, byte[]>> {

    @Mixin
    private RedisOptions targetRedis = new RedisOptions();
    @Mixin
    private RedisExportOptions options = new RedisExportOptions();
    @Option(names = "--live", description = "Enable live replication.")
    private boolean live;
    @Option(names = "--event-queue", description = "Capacity of the keyspace notification event queue (default: ${DEFAULT-VALUE}).", paramLabel = "<size>")
    private int notificationQueueCapacity = KeyValueItemReaderBuilder.DEFAULT_NOTIFICATION_QUEUE_CAPACITY;
    @Option(names = "--no-verify", description = "Verify target against source dataset after replication. True by default.", negatable = true)
    private boolean verify = true;
    @Option(names = "--ttl-tolerance", description = "Max TTL difference to use for dataset verification (default: ${DEFAULT-VALUE}).", paramLabel = "<sec>")
    private long ttlTolerance = KeyComparisonItemWriter.KeyComparisonItemWriterBuilder.DEFAULT_TTL_TOLERANCE.getSeconds();

    private AbstractRedisClient targetClient;

    @Override
    public void afterPropertiesSet() throws Exception {
        targetClient = targetRedis.client();
        super.afterPropertiesSet();
    }

    @Override
    public void shutdown() {
        super.shutdown();
        targetClient.shutdown();
        targetClient.getResources().shutdown();

    }

    @Override
    protected Flow flow() throws Exception {
        String name = "Scanning";
        FlowBuilder<SimpleFlow> flow = flowBuilder(name).start(step(name, sourceKeyDumpReader(), null, targetKeyDumpWriter()).build());
        if (live) {
            String liveName = "Listening";
            AbstractKeyDumpItemReader<String, String, ?> liveReader = liveReader();
            liveReader.setName("Live" + ClassUtils.getShortName(liveReader.getClass()));
            SimpleFlow liveFlow = flowBuilder(liveName).start(step(liveName, liveReader, null, targetKeyDumpWriter()).build()).build();
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
            return KeyComparisonItemWriter.builder(RedisClusterDataStructureItemReader.builder(targetRedisClusterPool(), ((RedisClusterClient) targetClient).connect()).commandTimeout(getTargetCommandTimeout()).build()).ttlTolerance(ttlToleranceDuration).build();
        }
        return KeyComparisonItemWriter.builder(RedisDataStructureItemReader.builder(targetRedisPool(), ((RedisClient) targetClient).connect()).commandTimeout(getTargetCommandTimeout()).build()).ttlTolerance(ttlToleranceDuration).build();
    }

    protected StatefulConnection<String, String> connection() {
        if (isCluster()) {
            return redisClusterConnection();
        }
        return redisConnection();
    }

    private ItemReader<KeyValue<String, byte[]>> sourceKeyDumpReader() {
        if (isCluster()) {
            return configureScanKeyValueItemReaderBuilder(RedisClusterKeyDumpItemReader.builder(redisClusterPool(), redisClusterConnection())).build();
        }
        return configureScanKeyValueItemReaderBuilder(RedisKeyDumpItemReader.builder(redisPool(), redisConnection())).build();
    }

    private ItemReader<DataStructure<String>> sourceDataStructureReader() {
        if (isCluster()) {
            return configureScanKeyValueItemReaderBuilder(RedisClusterDataStructureItemReader.builder(redisClusterPool(), redisClusterConnection())).build();
        }
        return configureScanKeyValueItemReaderBuilder(RedisDataStructureItemReader.builder(redisPool(), redisConnection())).build();
    }


    private AbstractKeyDumpItemReader<String, String, ?> liveReader() {
        if (isCluster()) {
            return configureNotificationKeyValueItemReaderBuilder(RedisClusterKeyDumpItemReader.builder(redisClusterPool(), getRedisClusterClient().connectPubSub())).build();
        }
        return configureNotificationKeyValueItemReaderBuilder(RedisKeyDumpItemReader.builder(redisPool(), getRedisClient().connectPubSub())).build();
    }

    private <B extends ScanKeyValueItemReaderBuilder<B>> B configureScanKeyValueItemReaderBuilder(B builder) {
        return configureSourceKeyValueItemReaderBuilder(builder.scanCount(options.getScanCount()));
    }

    private <B extends NotificationKeyValueItemReaderBuilder<B>> B configureNotificationKeyValueItemReaderBuilder(B builder) {
        return configureSourceKeyValueItemReaderBuilder(builder.notificationQueueCapacity(notificationQueueCapacity).database(getRedisURI().getDatabase()));
    }

    private <B extends KeyValueItemReaderBuilder<B>> B configureSourceKeyValueItemReaderBuilder(B builder) {
        return builder.keyPattern(options.getScanMatch()).threadCount(options.getThreads()).chunkSize(options.getBatchSize()).commandTimeout(getCommandTimeout()).queueCapacity(options.getQueueCapacity());
    }

    private ItemWriter<KeyValue<String, byte[]>> targetKeyDumpWriter() {
        if (targetRedis.isCluster()) {
            return RedisClusterKeyDumpItemWriter.builder(targetRedisClusterPool()).replace(true).commandTimeout(getTargetCommandTimeout()).build();
        }
        return RedisKeyDumpItemWriter.builder(targetRedisPool()).replace(true).commandTimeout(getTargetCommandTimeout()).build();
    }

    private Duration getTargetCommandTimeout() {
        return targetRedis.uris().get(0).getTimeout();
    }

    private GenericObjectPool<StatefulRedisConnection<String, String>> targetRedisPool() {
        return targetRedis.pool((RedisClient) targetClient);
    }

    private GenericObjectPool<StatefulRedisClusterConnection<String, String>> targetRedisClusterPool() {
        return targetRedis.pool((RedisClusterClient) targetClient);
    }

    @Override
    protected Long size() throws InterruptedException, ExecutionException, TimeoutException {
        if (isCluster()) {
            return configure(RedisClusterDatasetSizeEstimator.builder(redisClusterConnection())).build().call();
        }
        return configure(RedisDatasetSizeEstimator.builder(redisConnection())).build().call();
    }

    private <B extends DatasetSizeEstimatorBuilder<B>> B configure(B builder) {
        return builder.sampleSize(options.getSampleSize()).commandTimeout(getCommandTimeout().getSeconds()).keyPattern(options.getScanMatch());
    }
}
