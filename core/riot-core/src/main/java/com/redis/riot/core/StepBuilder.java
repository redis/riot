package com.redis.riot.core;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import org.springframework.batch.core.ItemWriteListener;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.step.builder.FaultTolerantStepBuilder;
import org.springframework.batch.core.step.builder.SimpleStepBuilder;
import org.springframework.batch.core.step.skip.AlwaysSkipItemSkipPolicy;
import org.springframework.batch.core.step.skip.LimitCheckingItemSkipPolicy;
import org.springframework.batch.core.step.skip.NeverSkipItemSkipPolicy;
import org.springframework.batch.core.step.skip.SkipPolicy;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemStreamReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.support.SynchronizedItemStreamReader;
import org.springframework.classify.BinaryExceptionClassifier;

import com.redis.spring.batch.RedisItemReader;
import com.redis.spring.batch.step.FlushingStepBuilder;
import com.redis.spring.batch.util.BatchUtils;

import io.lettuce.core.RedisException;

public class StepBuilder<I, O> {

    private final StepBuilderFactory factory;

    private final String name;

    private final ItemReader<I> reader;

    private final ItemWriter<O> writer;

    private ItemProcessor<I, O> processor;

    private List<StepExecutionListener> executionListeners = new ArrayList<>();

    private List<ItemWriteListener<O>> writeListeners = new ArrayList<>();

    private StepOptions options = new StepOptions();

    private Duration flushingInterval;

    private Duration idleTimeout;

    private Collection<Class<? extends Throwable>> skippableExceptions = new ArrayList<>();

    private Collection<Class<? extends Throwable>> retriableExceptions = defaultRetriableExceptions();

    public static Collection<Class<? extends Throwable>> defaultRetriableExceptions() {
        Collection<Class<? extends Throwable>> exceptions = new ArrayList<>();
        exceptions.add(RedisException.class);
        exceptions.add(TimeoutException.class);
        exceptions.add(ExecutionException.class);
        return exceptions;
    }

    private StepBuilder(StepBuilderFactory factory, String name, ItemReader<I> reader, ItemWriter<O> writer) {
        this.factory = factory;
        this.name = name;
        this.reader = reader;
        this.writer = writer;
    }

    public String getName() {
        return name;
    }

    public ItemReader<I> getReader() {
        return reader;
    }

    public ItemWriter<O> getWriter() {
        return writer;
    }

    @SuppressWarnings("unchecked")
    public StepBuilder<I, O> skippableExceptions(Class<? extends Throwable>... exceptions) {
        this.skippableExceptions = Arrays.asList(exceptions);
        return this;
    }

    @SuppressWarnings("unchecked")
    public StepBuilder<I, O> retriableExceptions(Class<? extends Throwable>... exceptions) {
        this.retriableExceptions = Arrays.asList(exceptions);
        return this;
    }

    public StepBuilder<I, O> options(StepOptions options) {
        this.options = options;
        return this;
    }

    public void addWriteListener(ItemWriteListener<O> listener) {
        this.writeListeners.add(listener);
    }

    public void addExecutionListener(StepExecutionListener listener) {
        this.executionListeners.add(listener);
    }

    public StepBuilder<I, O> processor(ItemProcessor<I, O> processor) {
        this.processor = processor;
        return this;
    }

    public StepBuilder<I, O> flushingInterval(Duration interval) {
        this.flushingInterval = interval;
        return this;
    }

    public StepBuilder<I, O> idleTimeout(Duration timeout) {
        this.idleTimeout = timeout;
        return this;
    }

    public SimpleStepBuilder<I, O> build() {
        SimpleStepBuilder<I, O> step = factory.get(name).chunk(options.getChunkSize());
        step.reader(reader());
        step.processor(processor);
        step.writer(writer());
        if (options.getThreads() > 1) {
            step.taskExecutor(BatchUtils.threadPoolTaskExecutor(options.getThreads()));
            step.throttleLimit(options.getThreads());
        }
        executionListeners.forEach(step::listener);
        writeListeners.forEach(step::listener);
        if (BatchUtils.isPositive(flushingInterval)) {
            step = new FlushingStepBuilder<>(step).interval(flushingInterval).idleTimeout(idleTimeout);
        }
        if (options.isFaultTolerance()) {
            step = retry(step.faultTolerant().skipPolicy(skipPolicy()).retryLimit(options.getRetryLimit()));
        }
        return step;
    }

    private FaultTolerantStepBuilder<I, O> retry(FaultTolerantStepBuilder<I, O> step) {
        retriableExceptions.forEach(step::retry);
        return step;
    }

    private ItemReader<I> reader() {
        if (reader instanceof RedisItemReader) {
            return reader;
        }
        if (options.getThreads() > 1 && reader instanceof ItemStreamReader) {
            SynchronizedItemStreamReader<I> synchronizedReader = new SynchronizedItemStreamReader<>();
            synchronizedReader.setDelegate((ItemStreamReader<I>) reader);
            return synchronizedReader;
        }
        return reader;
    }

    private ItemWriter<O> writer() {
        if (options.isDryRun()) {
            return new NoopItemWriter<>();
        }
        if (BatchUtils.isPositive(options.getSleep())) {
            return new ThrottledItemWriter<>(writer, options.getSleep());
        }
        return writer;
    }

    private static class NoopItemWriter<T> implements ItemWriter<T> {

        @Override
        public void write(List<? extends T> items) throws Exception {
            // Do nothing
        }

    }

    private SkipPolicy skipPolicy() {
        switch (options.getSkipPolicy()) {
            case ALWAYS:
                return new AlwaysSkipItemSkipPolicy();
            case NEVER:
                return new NeverSkipItemSkipPolicy();
            default:
                return new LimitCheckingItemSkipPolicy(options.getSkipLimit(),
                        new BinaryExceptionClassifier(skippableExceptions));
        }
    }

    public static NameBuilder factory(StepBuilderFactory factory) {
        return new NameBuilder(factory);
    }

    public static class NameBuilder {

        private final StepBuilderFactory factory;

        public NameBuilder(StepBuilderFactory factory) {
            this.factory = factory;
        }

        public ReaderStepBuilder name(String name) {
            return new ReaderStepBuilder(factory, name);
        }

    }

    public static class ReaderStepBuilder {

        private final StepBuilderFactory factory;

        private final String name;

        private StepOptions options = new StepOptions();

        public ReaderStepBuilder(StepBuilderFactory factory, String name) {
            this.factory = factory;
            this.name = name;
        }

        public ReaderStepBuilder options(StepOptions options) {
            this.options = options;
            return this;
        }

        public <I> Builder<I> reader(ItemReader<I> reader) {
            return new Builder<>(factory, name, reader).options(options);
        }

    }

    public static class Builder<I> {

        private final StepBuilderFactory factory;

        private final String name;

        private final ItemReader<I> reader;

        private StepOptions options = new StepOptions();

        public Builder(StepBuilderFactory factory, String name, ItemReader<I> reader) {
            this.factory = factory;
            this.name = name;
            this.reader = reader;
        }

        public Builder<I> options(StepOptions options) {
            this.options = options;
            return this;
        }

        public <O> StepBuilder<I, O> writer(ItemWriter<O> writer) {
            return new StepBuilder<>(factory, name, reader, writer).options(options);
        }

    }

}
