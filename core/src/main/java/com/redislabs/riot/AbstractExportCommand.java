package com.redislabs.riot;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisKeyAsyncCommands;
import io.lettuce.core.api.async.RedisServerAsyncCommands;
import org.springframework.batch.core.step.builder.AbstractTaskletStepBuilder;
import org.springframework.batch.core.step.builder.SimpleStepBuilder;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.redis.RedisClusterDataStructureItemReader;
import org.springframework.batch.item.redis.RedisDataStructureItemReader;
import org.springframework.batch.item.redis.support.DataStructure;
import org.springframework.batch.item.redis.support.GlobToRegexConverter;
import org.springframework.batch.item.redis.support.ScanKeyValueItemReaderBuilder;
import picocli.CommandLine.Mixin;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.regex.Pattern;

public abstract class AbstractExportCommand<O> extends AbstractTransferCommand<DataStructure<String>, O> {

    @Mixin
    private RedisExportOptions options = new RedisExportOptions();

    protected AbstractTaskletStepBuilder<SimpleStepBuilder<DataStructure<String>, O>> step(ItemProcessor<DataStructure<String>, O> processor, ItemWriter<O> writer) throws Exception {
        return step("Exporting from " + name(getRedisURI()), reader(), processor, writer);
    }

    protected final ItemReader<DataStructure<String>> reader() {
        if (isCluster()) {
            return configure(RedisClusterDataStructureItemReader.builder(redisClusterPool(), getRedisClusterClient().connect())).build();
        }
        return configure(RedisDataStructureItemReader.builder(redisPool(), getRedisClient().connect())).build();
    }

    private <B extends ScanKeyValueItemReaderBuilder<B>> B configure(B builder) {
        return builder.commandTimeout(getCommandTimeout()).chunkSize(options.getBatchSize()).queueCapacity(options.getQueueCapacity()).threads(options.getThreads()).keyPattern(options.getScanMatch()).scanCount(options.getScanCount());
    }

    @Override
    protected Long size() throws InterruptedException, ExecutionException, TimeoutException {
        long commandTimeout = getCommandTimeout().getSeconds();
        BaseRedisAsyncCommands<String, String> async = async();
        async.setAutoFlushCommands(false);
        RedisFuture<Long> dbsizeFuture = ((RedisServerAsyncCommands<String, String>) async).dbsize();
        List<RedisFuture<String>> keyFutures = new ArrayList<>(options.getSampleSize());
        // rough estimate of keys matching pattern
        for (int index = 0; index < options.getSampleSize(); index++) {
            keyFutures.add(((RedisKeyAsyncCommands<String, String>) async).randomkey());
        }
        async.flushCommands();
        async.setAutoFlushCommands(true);
        int matchCount = 0;
        Pattern pattern = Pattern.compile(GlobToRegexConverter.convert(options.getScanMatch()));
        for (RedisFuture<String> future : keyFutures) {
            String key = future.get(commandTimeout, TimeUnit.SECONDS);
            if (key == null) {
                continue;
            }
            if (pattern.matcher(key).matches()) {
                matchCount++;
            }
        }
        Long dbsize = dbsizeFuture.get(commandTimeout, TimeUnit.SECONDS);
        if (dbsize == null) {
            return null;
        }
        return dbsize * matchCount / options.getSampleSize();
    }

}
