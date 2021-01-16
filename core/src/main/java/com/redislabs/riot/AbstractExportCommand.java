package com.redislabs.riot;

import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.springframework.batch.core.step.builder.AbstractTaskletStepBuilder;
import org.springframework.batch.core.step.builder.SimpleStepBuilder;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.redis.RedisClusterDataStructureItemReader;
import org.springframework.batch.item.redis.RedisDataStructureItemReader;
import org.springframework.batch.item.redis.support.DataStructure;
import org.springframework.batch.item.redis.support.ScanKeyValueItemReaderBuilder;
import picocli.CommandLine;

public abstract class AbstractExportCommand<O> extends AbstractTransferCommand<DataStructure<String>, O> {

    @CommandLine.ArgGroup(exclusive = false, heading = "Redis reader options%n")
    private RedisReaderOptions options = RedisReaderOptions.builder().build();

    protected AbstractTaskletStepBuilder<SimpleStepBuilder<DataStructure<String>, O>> step(ItemProcessor<DataStructure<String>, O> processor, ItemWriter<O> writer) throws Exception {
        return step("Exporting from " + name(getRedisURI()), reader(), processor, writer);
    }

    protected final ItemReader<DataStructure<String>> reader() {
        if (isCluster()) {
            return configure(RedisClusterDataStructureItemReader.builder((GenericObjectPool<StatefulRedisClusterConnection<String, String>>) pool, (StatefulRedisClusterConnection<String, String>) connection)).build();
        }
        return configure(RedisDataStructureItemReader.builder((GenericObjectPool<StatefulRedisConnection<String, String>>) pool, (StatefulRedisConnection<String, String>) connection)).build();
    }

    private <B extends ScanKeyValueItemReaderBuilder<B>> B configure(B builder) {
        return builder.commandTimeout(getCommandTimeout()).chunkSize(options.getBatchSize()).queueCapacity(options.getQueueCapacity()).threadCount(options.getThreads()).keyPattern(options.getScanMatch()).scanCount(options.getScanCount());
    }

}
