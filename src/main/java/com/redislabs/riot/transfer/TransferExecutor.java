package com.redislabs.riot.transfer;

import lombok.Builder;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.item.ExecutionContext;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

@Slf4j
public class TransferExecutor<I, O> implements Runnable {

    public final static String CONTEXT_PARTITION = "partition";
    public final static String CONTEXT_PARTITIONS = "partitions";

    private final int id;
    private final Batcher<I, O> batcher;
    private final TransferExecution<I,O> execution;

    private long readCount;
    private long writeCount;
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
    private ScheduledFuture<?> flushFuture;
    private boolean running;
    private boolean stopped;

    @Builder
    public TransferExecutor(int id, TransferExecution<I, O> execution, Batcher<I, O> batcher) {
        super();
        this.id = id;
        this.execution = execution;
        this.batcher = batcher;
    }

    @Override
    public void run() {
        ExecutionContext executionContext = new ExecutionContext();
        executionContext.putInt(CONTEXT_PARTITION, id);
        executionContext.putInt(CONTEXT_PARTITIONS, execution.getOptions().getNThreads());
        try {
            execution.getTransfer().open(executionContext);
        } catch (Exception e) {
            log.error("Could not initialize transfer", e);
            return;
        }
        if (execution.getOptions().getFlushRate() != null) {
            flushFuture = scheduler.scheduleAtFixedRate(new Flusher(), execution.getOptions().getFlushRate(), execution.getOptions().getFlushRate(), TimeUnit.MILLISECONDS);
        }
        running = true;
        try {
            List<O> items;
            while ((items = batcher.next()) != null && !stopped) {
                write(items);
            }
        } catch (Exception e) {
            log.error("Error during transfer", e);
        } finally {
            if (flushFuture != null) {
                flushFuture.cancel(true);
            }
            scheduler.shutdown();
            try {
                execution.getTransfer().close();
            } catch (Exception e) {
                log.error("Could not close transfer", e);
            }
            this.running = false;
            log.debug("Transfer executor {} finished", id);
        }

    }

    public Metrics progress() {
        return Metrics.builder().reads(readCount).writes(writeCount).runningThreads(running ? 1 : 0).build();
    }

    public void stop() {
        stopped = true;
    }

    private void write(List<O> items) throws Exception {
        readCount += items.size();
        execution.getTransfer().write(items);
        writeCount += items.size();
    }

    private class Flusher implements Runnable {

        @Override
        public void run() {
            List<O> items = batcher.flush();
            if (!items.isEmpty()) {
                try {
                    TransferExecutor.this.write(items);
                } catch (Exception e) {
                    log.error("Could not flush transfer executor {}", TransferExecutor.this.id, e);
                }
            }
        }
    }

}
