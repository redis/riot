package com.redis.riot.core;

import java.util.function.Consumer;

import org.springframework.batch.core.step.builder.SimpleStepBuilder;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;

public class RiotStep<I, O> {

    private String name;

    private ItemReader<I> reader;

    private ItemProcessor<I, O> processor;

    private ItemWriter<O> writer;

    private Consumer<SimpleStepBuilder<I, O>> configurer = b -> {
    };

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public ItemReader<I> getReader() {
        return reader;
    }

    public void setReader(ItemReader<I> reader) {
        this.reader = reader;
    }

    public ItemProcessor<I, O> getProcessor() {
        return processor;
    }

    public void setProcessor(ItemProcessor<I, O> processor) {
        this.processor = processor;
    }

    public ItemWriter<O> getWriter() {
        return writer;
    }

    public void setWriter(ItemWriter<O> writer) {
        this.writer = writer;
    }

    public Consumer<SimpleStepBuilder<I, O>> getConfigurer() {
        return configurer;
    }

    public void setConfigurer(Consumer<SimpleStepBuilder<I, O>> configurer) {
        this.configurer = configurer;
    }

}
