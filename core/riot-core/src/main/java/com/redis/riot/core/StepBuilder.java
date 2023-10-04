package com.redis.riot.core;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.springframework.batch.core.ItemWriteListener;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.step.builder.FaultTolerantStepBuilder;
import org.springframework.batch.core.step.builder.SimpleStepBuilder;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemStreamReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.support.SynchronizedItemStreamReader;
import org.springframework.beans.factory.InitializingBean;

import com.redis.spring.batch.RedisItemReader;
import com.redis.spring.batch.step.FlushingStepBuilder;
import com.redis.spring.batch.util.BatchUtils;

import io.lettuce.core.RedisCommandExecutionException;
import io.lettuce.core.RedisCommandTimeoutException;

public class StepBuilder<I, O> {

    private final StepBuilderFactory factory;

    private String name;

    private ItemReader<I> reader;

    private ItemWriter<O> writer;

    private ItemProcessor<I, O> processor;

    private List<StepExecutionListener> executionListeners = new ArrayList<>();

    private List<ItemWriteListener<O>> writeListeners = new ArrayList<>();

    private StepOptions options = new StepOptions();

    private List<StepConfigurationStrategy> configurationStrategies = new ArrayList<>();

    private List<Class<? extends Throwable>> skippableExceptions = defaultNonRetriableExceptions();

    private List<Class<? extends Throwable>> nonSkippableExceptions = defaultRetriableExceptions();

    private List<Class<? extends Throwable>> retriableExceptions = defaultRetriableExceptions();

    private List<Class<? extends Throwable>> nonRetriableExceptions = defaultNonRetriableExceptions();

    @SuppressWarnings("unchecked")
    public static List<Class<? extends Throwable>> defaultRetriableExceptions() {
        return modifiableList(RedisCommandTimeoutException.class);
    }

    @SuppressWarnings("unchecked")
    public static List<Class<? extends Throwable>> defaultNonRetriableExceptions() {
        return modifiableList(RedisCommandExecutionException.class);
    }

    @SuppressWarnings("unchecked")
    private static <T> List<T> modifiableList(T... elements) {
        return new ArrayList<>(Arrays.asList(elements));
    }

    public StepBuilder(StepBuilderFactory factory) {
        this.factory = factory;
    }

    public void addSkippableException(Class<? extends Throwable> exception) {
        skippableExceptions.add(exception);
    }

    public void addNonSkippableException(Class<? extends Throwable> exception) {
        nonSkippableExceptions.add(exception);
    }

    public void addRetriableException(Class<? extends Throwable> exception) {
        retriableExceptions.add(exception);
    }

    public void addNonRetriableException(Class<? extends Throwable> exception) {
        nonRetriableExceptions.add(exception);
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

    public StepBuilder<I, O> reader(ItemReader<I> reader) {
        this.reader = reader;
        return this;
    }

    public StepBuilder<I, O> writer(ItemWriter<O> writer) {
        this.writer = writer;
        return this;
    }

    public StepBuilder<I, O> name(String name) {
        this.name = name;
        return this;
    }

    public StepBuilder<I, O> options(StepOptions options) {
        this.options = options;
        return this;
    }

    public void accept(StepConfigurationStrategy strategy) {
        strategy.configure(this);
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

    public FaultTolerantStepBuilder<I, O> build() {
        configurationStrategies.forEach(s -> s.configure(this));
        FaultTolerantStepBuilder<I, O> step = simpleStep().faultTolerant();
        step.reader(reader());
        step.processor(processor());
        step.writer(writer());
        if (options.getThreads() > 1) {
            step.taskExecutor(BatchUtils.threadPoolTaskExecutor(options.getThreads()));
            step.throttleLimit(options.getThreads());
        }
        executionListeners.forEach(step::listener);
        writeListeners.forEach(step::listener);
        step.skipLimit(options.getSkipLimit());
        step.retryLimit(options.getRetryLimit());
        skippableExceptions.forEach(step::skip);
        nonSkippableExceptions.forEach(step::noSkip);
        retriableExceptions.forEach(step::retry);
        nonRetriableExceptions.forEach(step::noRetry);
        return step;
    }

    private SimpleStepBuilder<I, O> simpleStep() {
        SimpleStepBuilder<I, O> step = factory.get(name).chunk(options.getChunkSize());
        if (reader instanceof RedisItemReader) {
            RedisItemReader<?, ?, ?> redisReader = (RedisItemReader<?, ?, ?>) reader;
            if (redisReader.isLive()) {
                FlushingStepBuilder<I, O> flushingStep = new FlushingStepBuilder<>(step);
                flushingStep.interval(redisReader.getFlushInterval());
                flushingStep.idleTimeout(redisReader.getIdleTimeout());
                return flushingStep;
            }
        }
        return step;
    }

    private ItemProcessor<? super I, ? extends O> processor() {
        initialize(processor);
        return processor;
    }

    private void initialize(Object object) {
        if (object instanceof InitializingBean) {
            try {
                ((InitializingBean) object).afterPropertiesSet();
            } catch (Exception e) {
                throw new RiotExecutionException("Could not initialize " + object, e);
            }
        }
    }

    private ItemReader<I> reader() {
        initialize(reader);
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
        initialize(writer);
        if (options.isDryRun()) {
            return new NoopItemWriter<>();
        }
        if (options.getSleep() == null || options.getSleep().isNegative() || options.getSleep().isZero()) {
            return writer;
        }
        return new ThrottledItemWriter<>(writer, options.getSleep());
    }

    private static class NoopItemWriter<T> implements ItemWriter<T> {

        @Override
        public void write(List<? extends T> items) throws Exception {
            // Do nothing
        }

    }

    public StepBuilder<I, O> configurationStrategies(List<StepConfigurationStrategy> strategies) {
        this.configurationStrategies = strategies;
        return this;
    }

}
