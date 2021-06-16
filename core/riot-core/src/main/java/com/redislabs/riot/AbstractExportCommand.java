package com.redislabs.riot;

import com.redislabs.mesclun.RedisModulesClient;
import io.lettuce.core.cluster.RedisClusterClient;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.step.builder.AbstractTaskletStepBuilder;
import org.springframework.batch.core.step.builder.SimpleStepBuilder;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.redis.DataStructureItemReader;
import org.springframework.batch.item.redis.support.DataStructure;
import picocli.CommandLine;

@Slf4j
public abstract class AbstractExportCommand<O> extends AbstractTransferCommand {

    @CommandLine.ArgGroup(exclusive = false, heading = "Redis reader options%n")
    private RedisReaderOptions options = new RedisReaderOptions();

    protected AbstractTaskletStepBuilder<SimpleStepBuilder<DataStructure, O>> step(StepBuilderFactory stepBuilderFactory, ItemProcessor<DataStructure, O> processor, ItemWriter<O> writer) throws Exception {
        String name = name(getRedisOptions().uris().get(0));
        StepBuilder stepBuilder = stepBuilderFactory.get(name + "-export-step");
        RiotStepBuilder<DataStructure, O> step = riotStep(stepBuilder, "Exporting from " + name);
        return step.reader(reader()).processor(processor).writer(writer).build();
    }

    protected final DataStructureItemReader reader() {
        RedisOptions redisOptions = getRedisOptions();
        if (redisOptions.isCluster()) {
            RedisClusterClient client = redisOptions.redisClusterClient();
            return options.configure(DataStructureItemReader.client(client)).build();
        }
        RedisModulesClient client = redisOptions.redisClient();
        return options.configure(DataStructureItemReader.client(client)).build();
    }

    @Override
    protected <I, O> RiotStepBuilder<I, O> riotStep(StepBuilder stepBuilder, String taskName) {
        RiotStepBuilder<I, O> riotStepBuilder = super.riotStep(stepBuilder, taskName);
        riotStepBuilder.initialMax(options.initialMaxSupplier(getRedisOptions()));
        return riotStepBuilder;
    }

}
