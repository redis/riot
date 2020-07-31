package com.redislabs.riot;

import io.lettuce.core.RedisURI;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.redis.support.RedisConnectionBuilder;
import org.springframework.batch.item.redisearch.support.RediSearchConnectionBuilder;
import org.springframework.batch.item.support.CompositeItemProcessor;
import org.springframework.batch.item.support.PassThroughItemProcessor;
import picocli.CommandLine;

import java.util.Collections;
import java.util.List;

@Slf4j
@CommandLine.Command(abbreviateSynopsis = true, sortOptions = false)
public abstract class AbstractTransferCommand<I, O> extends HelpCommand implements Runnable {

    @CommandLine.ParentCommand
    private RiotApp app;
    @CommandLine.Option(names = "--threads", description = "Thread count (default: ${DEFAULT-VALUE})", paramLabel = "<int>")
    private int threads = 1;
    @CommandLine.Option(names = {"-b", "--batch"}, description = "Number of items in each batch (default: ${DEFAULT-VALUE})", paramLabel = "<size>")
    private int batchSize = 50;
    @CommandLine.Option(names = "--max", description = "Max number of items to read", paramLabel = "<count>")
    private Integer maxItemCount;

    public RedisConnectionOptions getRedisConnectionOptions() {
        return app.getRedisConnectionOptions();
    }

    protected <B extends RedisConnectionBuilder<B>> B configure(RedisConnectionBuilder<B> builder) {
        return app.configure(builder);
    }

    protected <B extends RediSearchConnectionBuilder<B>> B configure(RediSearchConnectionBuilder<B> builder) {
        return app.configure(builder);
    }

    protected <B extends RedisConnectionBuilder<B>> B configure(RedisConnectionBuilder<B> builder, RedisConnectionOptions redis) {
        return app.configure(builder, redis);
    }

    protected <B extends RediSearchConnectionBuilder<B>> B configure(RediSearchConnectionBuilder<B> builder, RedisConnectionOptions redis) {
        return app.configure(builder, redis);
    }

    protected Long flushPeriod() {
        return null;
    }

    @Override
    public void run() {
        List<Transfer<I, O>> transfers;
        try {
            transfers = transfers();
        } catch (Exception e) {
            log.error("Could not create transfers", e);
            return;
        }
        execute(transfers);
    }

    protected Transfer<I, O> transfer(String name, ItemReader<I> reader, ItemProcessor<I, O> processor, ItemWriter<O> writer) {
        Transfer<I, O> transfer = new Transfer<>(name, reader, processor, writer);
        transfer.setBatchSize(batchSize);
        transfer.setThreadCount(threads);
        transfer.setMaxItemCount(maxItemCount);
        transfer.setFlushPeriod(flushPeriod());
        return transfer;
    }

    protected List<Transfer<I, O>> transfers(String name, ItemReader<I> reader, ItemProcessor<I, O> processor, ItemWriter<O> writer) {
        return Collections.singletonList(transfer(name, reader, processor, writer));
    }

    private void execute(List<Transfer<I, O>> transfers) {
        for (Transfer<I, O> transfer : transfers) {
            ProgressBarReporter reporter = ProgressBarReporter.builder().progressProvider(new TransferProgressProvider(transfer)).taskName(transfer.getName()).initialMax(maxItemCount).quiet(app.isQuiet()).build();
            reporter.start();
            execute(transfer);
            reporter.stop();
        }
    }

    protected abstract List<Transfer<I, O>> transfers() throws Exception;

    public void execute() throws Exception {
        execute(transfers());
    }

    public void execute(Transfer<I, O> transfer) {
        try {
            transfer.open();
        } catch (Exception e) {
            log.error("Could not start transfer", e);
            return;
        }
        try {
            transfer.execute();
        } catch (Exception e) {
            log.error("Could not execute transfer", e);
        } finally {
            transfer.close();
        }
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    protected ItemProcessor compositeProcessor(List<ItemProcessor> allProcessors) {
        if (allProcessors.isEmpty()) {
            return new PassThroughItemProcessor();
        }
        if (allProcessors.size() == 1) {
            return allProcessors.get(0);
        }
        CompositeItemProcessor compositeItemProcessor = new CompositeItemProcessor();
        compositeItemProcessor.setDelegates(allProcessors);
        return compositeItemProcessor;
    }


    private class TransferProgressProvider implements ProgressProvider {

        private final Transfer<I, O> transfer;

        public TransferProgressProvider(Transfer<I, O> transfer) {
            this.transfer = transfer;
        }

        @Override
        public long getWriteCount() {
            return transfer.getWriteCount();
        }
    }

    private class MultiTransferProgressProvider implements ProgressProvider {

        private final List<Transfer<I, O>> transfers;

        public MultiTransferProgressProvider(List<Transfer<I, O>> transfers) {
            this.transfers = transfers;
        }

        @Override
        public long getWriteCount() {
            return transfers.stream().map(Transfer::getWriteCount).mapToLong(Long::longValue).sum();
        }
    }


    protected String toString(RedisURI redisURI) {
        if (redisURI.getSocket() != null) {
            return redisURI.getSocket();
        }
        if (redisURI.getSentinelMasterId() != null) {
            return redisURI.getSentinelMasterId();
        }
        return redisURI.getHost();
    }
}
