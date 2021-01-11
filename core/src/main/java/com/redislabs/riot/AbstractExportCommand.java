package com.redislabs.riot;

import org.springframework.batch.core.step.builder.AbstractTaskletStepBuilder;
import org.springframework.batch.core.step.builder.SimpleStepBuilder;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.redis.RedisClusterDataStructureItemReader;
import org.springframework.batch.item.redis.RedisDataStructureItemReader;
import org.springframework.batch.item.redis.support.*;
import picocli.CommandLine.Mixin;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

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
        return builder.commandTimeout(getCommandTimeout()).chunkSize(options.getBatchSize()).queueCapacity(options.getQueueCapacity()).threadCount(options.getThreads()).keyPattern(options.getScanMatch()).scanCount(options.getScanCount());
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
