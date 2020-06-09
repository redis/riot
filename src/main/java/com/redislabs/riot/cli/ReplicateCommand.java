package com.redislabs.riot.cli;

import com.redislabs.picocliredis.RedisOptions;
import com.redislabs.riot.Riot;
import com.redislabs.riot.TransferOptions;
import io.lettuce.core.RedisClient;
import io.lettuce.core.cluster.RedisClusterClient;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.redis.*;
import org.springframework.batch.item.redis.support.KeyValue;
import org.springframework.batch.item.redis.support.ReaderOptions;
import org.springframework.batch.item.support.PassThroughItemProcessor;
import picocli.CommandLine;
import picocli.CommandLine.Command;

@Command(name = "replicate", description = "Replicate a Redis database to another Redis database")
public class ReplicateCommand extends TransferCommand {

    @CommandLine.ParentCommand
    private Riot riot;
    @CommandLine.Mixin
    private RedisOptions target = new RedisOptions();
    @CommandLine.Mixin
    private ExportOptions options = new ExportOptions();
    @CommandLine.Option(names = "--keyspace-patterns", description = "Keyspace notification subscription patterns", paramLabel = "<str>", hidden = true)
    private String[] keyspacePatterns;
    @CommandLine.Option(names = "--flush-interval", description = "Duration between notification flushes (default: ${DEFAULT-VALUE})", paramLabel = "<ms>")
    private long flushPeriod = 50;
    @CommandLine.Option(names = "--live", description = "Live replication")
    private boolean live;

    @Override
    protected boolean isQuiet() {
        return riot.isQuiet();
    }

    @Override
    protected TransferOptions transferOptions() {
        TransferOptions transferOptions = super.transferOptions();
        if (live) {
            transferOptions.setFlushPeriod(flushPeriod);
        }
        return transferOptions;
    }

    private ItemReader<KeyValue<String, byte[]>> reader() {
        if (riot.getRedisOptions().isCluster()) {
            RedisClusterClient redisClusterClient = riot.getRedisOptions().redisClusterClient();
            return RedisClusterKeyDumpItemReader.<String, String>builder().pool(connectionPool(redisClusterClient)).keyReader(keyReader(redisClusterClient)).options(readerOptions()).build();
        }
        RedisClient redisClient = riot.getRedisOptions().redisClient();
        return RedisKeyDumpItemReader.<String, String>builder().pool(connectionPool(redisClient)).keyReader(keyReader(redisClient)).options(readerOptions()).build();
    }

    private ReaderOptions readerOptions() {
        return ReaderOptions.builder().batchSize(options.getBatchSize()).commandTimeout(riot.getRedisOptions().getCommandTimeout()).threadCount(options.getThreads()).queueCapacity(options.getQueueCapacity()).build();
    }

    private ItemReader<String> keyReader(RedisClient redisClient) {
        if (live) {
            return RedisLiveKeyItemReader.builder().redisClient(redisClient).scanCount(options.getScanCount()).scanPattern(options.getScanMatch()).database(riot.getRedisOptions().getRedisURI().getDatabase()).pubSubPatterns(keyspacePatterns).queueCapacity(options.getQueueCapacity()).build();
        }
        return RedisKeyItemReader.builder().redisClient(redisClient).scanCount(options.getScanCount()).scanPattern(options.getScanMatch()).build();
    }


    private ItemReader<String> keyReader(RedisClusterClient redisClusterClient) {
        if (live) {
            return RedisClusterLiveKeyItemReader.builder().redisClusterClient(redisClusterClient).scanCount(options.getScanCount()).scanPattern(options.getScanMatch()).database(riot.getRedisOptions().getRedisURI().getDatabase()).pubSubPatterns(keyspacePatterns).queueCapacity(options.getQueueCapacity()).build();
        }
        return RedisClusterKeyItemReader.builder().redisClusterClient(redisClusterClient).scanCount(options.getScanCount()).scanPattern(options.getScanMatch()).build();
    }

    @Override
    public void run() {
        execute("Replicating", reader(), new PassThroughItemProcessor<>(), writer());
    }

    private ItemWriter<KeyValue<String, byte[]>> writer() {
        if (target.isCluster()) {
            return RedisClusterKeyDumpItemWriter.<String, String>builder().pool(connectionPool(target.redisClusterClient())).commandTimeout(target.getCommandTimeout()).replace(true).build();
        }
        return RedisKeyDumpItemWriter.<String, String>builder().pool(connectionPool(target.redisClient())).commandTimeout(target.getCommandTimeout()).replace(true).build();
    }

}
