package com.redislabs.riot;

import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.springframework.batch.core.step.builder.AbstractTaskletStepBuilder;
import org.springframework.batch.core.step.builder.SimpleStepBuilder;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.redis.DataStructureItemReader;
import org.springframework.batch.item.redis.support.DataStructure;
import org.springframework.batch.item.redis.support.ScanKeyValueItemReaderBuilder;
import picocli.CommandLine;

public abstract class AbstractExportCommand<O> extends AbstractTransferCommand<DataStructure<String>, O> {

    @CommandLine.ArgGroup(exclusive = false, heading = "Redis reader options%n")
    private RedisReaderOptions options = RedisReaderOptions.builder().build();

    protected AbstractTaskletStepBuilder<SimpleStepBuilder<DataStructure<String>, O>> step(ItemProcessor<DataStructure<String>, O> processor, ItemWriter<O> writer) throws Exception {
        String name = name(getRedisURI());
        StepBuilder<DataStructure<String>, O> step = stepBuilder(name + "-export-step", "Exporting from " + name);
        return step.reader(reader()).processor(processor).writer(writer).build();
    }

    protected final ItemReader<DataStructure<String>> reader() {
        if (isCluster()) {
            return configureScanKeyValueReaderBuilder(DataStructureItemReader.builder((GenericObjectPool<StatefulRedisClusterConnection<String, String>>) pool, (StatefulRedisClusterConnection<String, String>) connection)).build();
        }
        return configureScanKeyValueReaderBuilder(DataStructureItemReader.builder((GenericObjectPool<StatefulRedisConnection<String, String>>) pool, (StatefulRedisConnection<String, String>) connection)).build();
    }

    private ScanKeyValueItemReaderBuilder<DataStructureItemReader<String, String>> configureScanKeyValueReaderBuilder(ScanKeyValueItemReaderBuilder<DataStructureItemReader<String, String>> builder) {
        return builder.chunkSize(options.getBatchSize()).queueCapacity(options.getQueueCapacity()).threadCount(options.getThreads()).scanMatch(options.getScanMatch()).scanCount(options.getScanCount());
    }

}
