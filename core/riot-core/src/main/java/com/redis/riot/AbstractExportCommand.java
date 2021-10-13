package com.redis.riot;

import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.step.builder.AbstractTaskletStepBuilder;
import org.springframework.batch.core.step.builder.SimpleStepBuilder;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.redis.DataStructureItemReader;
import org.springframework.batch.item.redis.support.DataStructure;
import org.springframework.batch.item.redis.support.DataStructureValueReader;

import io.lettuce.core.AbstractRedisClient;
import picocli.CommandLine;

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
        AbstractRedisClient client = getRedisOptions().client();
        return options.configure(new DataStructureItemReader.DataStructureItemReaderBuilder(client, new DataStructureValueReader.DataStructureValueReaderBuilder(client).build())).build();
    }

    @Override
    protected <S, T> RiotStepBuilder<S, T> riotStep(StepBuilder stepBuilder, String taskName) {
        RiotStepBuilder<S, T> riotStepBuilder = super.riotStep(stepBuilder, taskName);
        riotStepBuilder.initialMax(options.initialMaxSupplier(getRedisOptions()));
        return riotStepBuilder;
    }

}
