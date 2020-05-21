package com.redislabs.riot.transfer;

import lombok.Builder;
import lombok.Data;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@Slf4j
public class TransferExecution<I, O> implements MetricsProvider {

    @Data
    @Builder
    public static class Options {

        private Integer maxItemCount;
        private int nThreads;
        private int batchSize;
        private Long flushRate;
        private ErrorHandler errorHandler;

    }

    @Getter
    private final Transfer<I, O> transfer;
    @Getter
    private final Options options;

    private List<TransferExecutor<I, O>> threads;
    private ExecutorService executor;
    private List<Future<?>> futures;

    public TransferExecution(Transfer<I, O> transfer, Options options) {
        this.transfer = transfer;
        this.options = options;
    }

    public void stop() {
        threads.forEach(t -> t.stop());
    }

    public boolean isTerminated() {
        return executor.isTerminated();
    }

    public void awaitTermination(long timeout, TimeUnit unit) {
        try {
            if (!executor.awaitTermination(timeout, unit)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
        }
    }

    public TransferExecution<I, O> execute() {
        this.threads = new ArrayList<>(options.getNThreads());
        ItemReader<I> reader = transfer.getReader();
        if (options.getMaxItemCount() != null) {
            if (reader instanceof AbstractItemCountingItemStreamItemReader) {
                ((AbstractItemCountingItemStreamItemReader<I>) reader).setMaxItemCount(options.getMaxItemCount());
            }
        }
        for (int index = 0; index < options.getNThreads(); index++) {
            Batcher<I, O> batcher = Batcher.<I, O>builder().reader(reader).processor(transfer.getProcessor()).batchSize(options.getBatchSize()).errorHandler(options.getErrorHandler()).build();
            threads.add(TransferExecutor.<I, O>builder().id(index).execution(this).batcher(batcher).build());
        }
        this.executor = Executors.newFixedThreadPool(threads.size());
        this.futures = new ArrayList<>(threads.size());
        for (TransferExecutor<I, O> thread : threads) {
            this.futures.add(executor.submit(thread));
        }
        executor.shutdown();
        return this;
    }

    @Override
    public Metrics getMetrics() {
        return Metrics.builder().metrics(threads.stream().map(TransferExecutor::progress).collect(Collectors.toList())).build();
    }
}