package com.redislabs.riot.redis;

import com.redislabs.riot.AbstractFlushingTransferCommand;
import com.redislabs.riot.RedisExportOptions;
import com.redislabs.riot.RedisOptions;
import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.RedisClient;
import io.lettuce.core.cluster.RedisClusterClient;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.batch.core.job.flow.support.SimpleFlow;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.redis.RedisClusterKeyDumpItemReader;
import org.springframework.batch.item.redis.RedisClusterKeyDumpItemWriter;
import org.springframework.batch.item.redis.RedisKeyDumpItemReader;
import org.springframework.batch.item.redis.RedisKeyDumpItemWriter;
import org.springframework.batch.item.redis.support.*;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.util.ClassUtils;
import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Option;

@Command(name = "replicate", aliases = "r", description = "Replicate a source Redis database in a target Redis database")
public class ReplicateCommand extends AbstractFlushingTransferCommand<KeyValue<String, byte[]>, KeyValue<String, byte[]>> {

    @Mixin
    private RedisOptions targetRedis = new RedisOptions();
    @Mixin
    private RedisExportOptions options = new RedisExportOptions();
    @Option(names = "--live", description = "Enable live replication")
    private boolean live;
    @Option(names = "--notif-queue", description = "Capacity of the keyspace notification queue (default: ${DEFAULT-VALUE})", paramLabel = "<int>")
    private int notificationQueueCapacity = KeyValueItemReaderBuilder.DEFAULT_NOTIFICATION_QUEUE_CAPACITY;

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
        String name = "Scanning " + options.getScanMatch();
        SimpleFlow flow = flowBuilder(name).start(step(name, reader(), null, writer()).build()).build();
        if (live) {
            String liveName = "Listening";
            AbstractKeyDumpItemReader<String, String, ?> liveReader = liveReader();
            liveReader.setName("Live" + ClassUtils.getShortName(liveReader.getClass()));
            SimpleFlow liveFlow = flowBuilder(liveName).start(step(liveName, liveReader, null, writer()).build()).build();
            return flowBuilder(liveName).split(new SimpleAsyncTaskExecutor()).add(flow, liveFlow).build();
        }
        return flow;
    }

    private ItemReader<KeyValue<String, byte[]>> reader() {
        if (isCluster()) {
            return configureScan(RedisClusterKeyDumpItemReader.builder(redisClusterPool(), redisClusterConnection())).build();
        }
        return configureScan(RedisKeyDumpItemReader.builder(redisPool(), redisConnection())).build();
    }

    private AbstractKeyDumpItemReader<String, String, ?> liveReader() {
        if (isCluster()) {
            return configureNotification(RedisClusterKeyDumpItemReader.builder(redisClusterPool(), getRedisClusterClient().connectPubSub())).build();
        }
        return configureNotification(RedisKeyDumpItemReader.builder(redisPool(), getRedisClient().connectPubSub())).build();
    }

    private <B extends NotificationKeyValueItemReaderBuilder<B>> B configureNotification(B builder) {
        return configure(builder.queueCapacity(notificationQueueCapacity).database(getRedisURI().getDatabase()));
    }

    private <B extends ScanKeyValueItemReaderBuilder<B>> B configureScan(B builder) {
        return configure(builder.scanCount(options.getScanCount()));
    }

    private <B extends KeyValueItemReaderBuilder<B>> B configure(B builder) {
        return builder.keyPattern(options.getScanMatch()).threads(options.getThreads()).chunkSize(options.getBatchSize()).commandTimeout(targetRedis.uri().getTimeout()).queueCapacity(options.getQueueCapacity());
    }

    private ItemWriter<KeyValue<String, byte[]>> writer() {
        if (targetRedis.isCluster()) {
            return RedisClusterKeyDumpItemWriter.builder(targetRedis.pool((RedisClusterClient) targetClient)).replace(true).commandTimeout(getCommandTimeout()).build();
        }
        return RedisKeyDumpItemWriter.builder(targetRedis.pool((RedisClient) targetClient)).replace(true).commandTimeout(getCommandTimeout()).build();
    }

}
