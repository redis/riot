package com.redislabs.riot;

import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.item.*;
import org.springframework.batch.item.redis.support.AbstractRedisItemReader;
import org.springframework.batch.item.redis.support.BatchRunnable;
import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;

import java.util.ArrayList;
import java.util.concurrent.*;

@Slf4j
public class Transfer<I, O> {

    private final ItemReader<I> reader;
    private final ItemProcessor<I, O> processor;
    private final ItemWriter<O> writer;
    private final TransferOptions options;
    private final ExecutorService executor;
    private final ArrayList<BatchRunnable<I>> threads;

    @Builder
    public Transfer(ItemReader<I> reader, ItemProcessor<I, O> processor, ItemWriter<O> writer, TransferOptions options) {
        this.reader = reader;
        this.processor = processor;
        this.writer = writer;
        this.options = options;
        this.executor = Executors.newFixedThreadPool(options.getThreadCount());
        this.threads = new ArrayList<>(options.getThreadCount());
    }

    public void execute() {
        ExecutionContext executionContext = new ExecutionContext();
        if (writer instanceof ItemStream) {
            log.debug("Opening writer");
            ((ItemStream) writer).open(executionContext);
        }
        if (reader instanceof ItemStream) {
            if (options.getMaxItemCount() != null) {
                if (reader instanceof AbstractItemCountingItemStreamItemReader) {
                    log.debug("Configuring reader with maxItemCount={}", options.getMaxItemCount());
                    ((AbstractItemCountingItemStreamItemReader<I>) reader).setMaxItemCount(options.getMaxItemCount());
                }
            }
            log.debug("Opening reader");
            ((ItemStream) reader).open(executionContext);
        }
        for (int index = 0; index < options.getThreadCount(); index++) {
            threads.add(new BatchRunnable<>(reader, new ProcessingItemWriter<>(processor, writer), options.getBatchSize()));
        }
        threads.forEach(executor::submit);
        executor.shutdown();
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        ScheduledFuture<?> scheduledFuture = null;
        if (options.getFlushPeriod() != null) {
            scheduledFuture = scheduler.scheduleAtFixedRate(this::flush, options.getFlushPeriod(), options.getFlushPeriod(), TimeUnit.MILLISECONDS);
        }
        try {
            while (!executor.isTerminated()) {
                try {
                    executor.awaitTermination(100, TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                    log.debug("Interrupted while awaiting termination", e);
                    throw new RuntimeException(e);
                }
            }
        } finally {
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
        }
    }

    private void flush() {
        if (reader instanceof AbstractRedisItemReader) {
            ((AbstractRedisItemReader<?, ?, ?>) reader).flush();
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
}
