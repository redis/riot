package com.redislabs.riot;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.item.*;
import org.springframework.batch.item.redis.support.BatchRunnable;
import org.springframework.batch.item.redis.support.RedisItemReader;
import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.batch.item.support.SynchronizedItemStreamReader;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

@Slf4j
public class Transfer<I, O> implements BatchRunnable.Listener {

    private final List<Listener> listeners = new ArrayList<>();
    @Getter
    private final ItemReader<I> reader;
    @Getter
    private final ItemProcessor<I, O> processor;
    @Getter
    private final ItemWriter<O> writer;
    private final int threadCount;
    private final int batchSize;
    @Setter
    private Long flushPeriod;
    private final Integer maxItemCount;
    private final ArrayList<BatchRunnable<I>> threads;
    private final ExecutorService executor;
    private final ScheduledExecutorService scheduler;
    private ScheduledFuture<?> scheduledFuture = null;

    @Builder
    public Transfer(ItemReader<I> reader, ItemProcessor<I, O> processor, ItemWriter<O> writer, int threadCount, int batchSize, Long flushPeriod, Integer maxItemCount) {
        this.reader = reader;
        this.processor = processor;
        this.writer = writer;
        this.threadCount = threadCount;
        this.batchSize = batchSize;
        this.flushPeriod = flushPeriod;
        this.maxItemCount = maxItemCount;
        this.threads = new ArrayList<>(threadCount);
        this.executor = Executors.newFixedThreadPool(threadCount);
        this.scheduler = Executors.newSingleThreadScheduledExecutor();
    }

    public void addListener(Listener listener) {
        listeners.add(listener);
    }

    public void open() {
        ExecutionContext executionContext = new ExecutionContext();
        if (writer instanceof ItemStream) {
            log.debug("Opening writer");
            ((ItemStream) writer).open(executionContext);
        }
        if (reader instanceof ItemStream) {
            if (maxItemCount != null) {
                if (reader instanceof AbstractItemCountingItemStreamItemReader) {
                    log.debug("Configuring reader with maxItemCount={}", maxItemCount);
                    ((AbstractItemCountingItemStreamItemReader<I>) reader).setMaxItemCount(maxItemCount);
                }
            }
            log.debug("Opening reader");
            ((ItemStream) reader).open(executionContext);
        }
        listeners.forEach(Listener::onOpen);
    }

    public void execute() {
        for (int index = 0; index < threadCount; index++) {
            threads.add(new BatchRunnable<>(reader(), new ProcessingItemWriter<>(processor, writer), batchSize));
        }
        threads.forEach(executor::submit);
        executor.shutdown();
        if (flushPeriod != null) {
            scheduledFuture = scheduler.scheduleAtFixedRate(this::flush, flushPeriod, flushPeriod, TimeUnit.MILLISECONDS);
        }
        while (!executor.isTerminated()) {
            try {
                executor.awaitTermination(100, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                log.debug("Interrupted while awaiting termination", e);
                throw new RuntimeException(e);
            }
        }
    }

    public void stop() {
        threads.forEach(BatchRunnable::stop);
    }

    public void close() {
        scheduler.shutdown();
        if (scheduledFuture != null) {
            scheduledFuture.cancel(false);
        }
        if (reader instanceof ItemStream) {
            log.debug("Closing reader");
            ((ItemStream) reader).close();
        }
        if (writer instanceof ItemStream) {
            log.debug("Closing writer");
            ((ItemStream) writer).close();
        }
        listeners.forEach(Listener::onClose);
    }

    private ItemReader<I> reader() {
        if (threadCount > 1 && reader instanceof ItemStreamReader) {
            SynchronizedItemStreamReader<I> synchronizedReader = new SynchronizedItemStreamReader<>();
            synchronizedReader.setDelegate((ItemStreamReader<I>) reader);
            return synchronizedReader;
        }
        return reader;
    }

    private void flush() {
        if (reader instanceof RedisItemReader) {
            ((RedisItemReader<?, ?>) reader).flush();
        }
        for (BatchRunnable<I> thread : threads) {
            try {
                thread.flush();
            } catch (Exception e) {
                log.error("Could not flush", e);
            }
        }
    }

    public long getWriteCount() {
        return threads.stream().mapToLong(BatchRunnable::getWriteCount).sum();
    }

    @Override
    public void onWrite(long writeCount) {
        listeners.forEach(l -> l.onUpdate(getWriteCount()));
    }

    public interface Listener {

        void onOpen();

        void onUpdate(long writeCount);

        void onClose();

    }
}
