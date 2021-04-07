package com.redislabs.riot;

import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.springframework.batch.core.step.builder.AbstractTaskletStepBuilder;
import org.springframework.batch.core.step.builder.SimpleStepBuilder;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.redis.RedisClusterDataStructureItemReader;
import org.springframework.batch.item.redis.RedisDataStructureItemReader;
import org.springframework.batch.item.redis.support.DataStructure;
import picocli.CommandLine;

import java.util.function.Supplier;

@Slf4j
public abstract class AbstractExportCommand<O> extends AbstractTransferCommand {

    @CommandLine.ArgGroup(exclusive = false, heading = "Redis reader options%n")
    private RedisReaderOptions options = RedisReaderOptions.builder().build();

    protected AbstractTaskletStepBuilder<SimpleStepBuilder<DataStructure<String>, O>> step(ItemProcessor<DataStructure<String>, O> processor, ItemWriter<O> writer) throws Exception {
        String name = name(getRedisURI());
        StepBuilder<DataStructure<String>, O> step = stepBuilder(name + "-export-step", "Exporting from " + name);
        return step.reader(reader()).processor(processor).writer(writer).build();
    }

    @SuppressWarnings("unchecked")
    protected final ItemReader<DataStructure<String>> reader() {
        if (connection instanceof StatefulRedisClusterConnection) {
            return options.configure(RedisClusterDataStructureItemReader.builder((GenericObjectPool<StatefulRedisClusterConnection<String, String>>) pool, (StatefulRedisClusterConnection<String, String>) connection)).build();
        }
        return options.configure(RedisDataStructureItemReader.builder((GenericObjectPool<StatefulRedisConnection<String, String>>) pool, (StatefulRedisConnection<String, String>) connection)).build();
    }

    @Override
    protected Supplier<Long> initialMax() {
        return initialMax(options.sizeEstimatorOptions());
    }
}
