package com.redislabs.riot.cli;

import com.redislabs.picocliredis.RedisCommandLineOptions;
import com.redislabs.riot.transfer.*;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.redis.support.PoolOptions;
import org.springframework.batch.item.redis.support.RedisOptions;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

@Slf4j
@Command
public abstract class TransferCommand<I, O> extends RiotCommand {

    @Option(names = "--threads", description = "Thread count (default: ${DEFAULT-VALUE})", paramLabel = "<count>")
    private int threads = 1;
    @Option(names = {"-b", "--batch"}, description = "Number of items in each batch (default: ${DEFAULT-VALUE})", paramLabel = "<size>")
    private int batchSize = 50;
    @Option(names = {"-m", "--max"}, description = "Max number of items to read", paramLabel = "<count>")
    private Long maxItemCount;
    @Option(names = "--sleep", description = "Sleep duration in millis between reads", paramLabel = "<ms>")
    private Long sleep;
    @Option(names = "--progress", description = "Progress reporting interval (default: ${DEFAULT-VALUE} ms)", paramLabel = "<ms>")
    private long progressRate = 300;
    @Option(names = "--max-wait", description = "Max duration to wait for transfer to complete", paramLabel = "<ms>")
    private Long maxWait;
    @Option(names = "--pool-size", description = "Max size of Redis connection pool (default: #threads)", paramLabel = "<int>")
    private Integer maxTotal;

    protected int getPoolMaxTotal() {
        if (maxTotal == null) {
            return threads;
        }
        return maxTotal;
    }

    @Override
    public void run() {
        ItemReader<I> reader;
        try {
            reader = reader();
        } catch (Exception e) {
            log.error("Could not initialize reader", e);
            return;
        }
        ItemProcessor<I, O> processor;
        try {
            processor = processor();
        } catch (Exception e) {
            log.error("Could not initialize processor", e);
            return;
        }
        ItemWriter<O> writer;
        try {
            writer = writer();
        } catch (Exception e) {
            log.error("Could not initialize writer", e);
            return;
        }
        Transfer<I, O> transfer = Transfer.<I, O>builder().reader(throttle(cap(reader))).processor(processor).writer(writer).errorHandler(errorHandler()).batchSize(batchSize).nThreads(threads).build();
        TransferExecution<I, O> execution = transfer.execute();
        if (!isQuiet()) {
            transfer.addListener(ProgressBarReporter.builder().taskName(taskName()).initialMax(maxItemCount).period(progressRate).timeUnit(TimeUnit.MILLISECONDS).metricsProvider(execution).build());
        }
        execution.awaitTermination(maxWait(), TimeUnit.MILLISECONDS);
    }

    protected abstract ItemWriter<O> writer() throws Exception;

    protected abstract ItemProcessor<I, O> processor() throws IOException, Exception;

    protected abstract ItemReader<I> reader() throws IOException, Exception;

    protected abstract String taskName();

    protected RedisOptions redisOptions() {
        return redisOptions(getOptions());
    }

    protected RedisOptions redisOptions(RedisCommandLineOptions cliOptions) {
        return RedisOptions.builder().clientOptions(cliOptions.clientOptions()).clusterClientOptions(cliOptions.clusterClientOptions()).clientResources(cliOptions.clientResources()).poolOptions(poolOptions()).cluster(cliOptions.isCluster()).redisURI(cliOptions.getRedisURI()).build();
    }

    private PoolOptions poolOptions() {
        return PoolOptions.builder().maxTotal(getPoolMaxTotal()).build();
    }

    protected ErrorHandler errorHandler() {
        return e -> log.error("Could not read item", e);
    }

    private long maxWait() {
        if (maxWait == null) {
            return Long.MAX_VALUE;
        }
        return maxWait;
    }

    private <I> ItemReader<I> throttle(ItemReader<I> reader) {
        if (sleep == null) {
            return reader;
        }
        return ThrottledReader.<I>builder().reader(reader).sleep(sleep).build();
    }

    private <I> ItemReader<I> cap(ItemReader<I> reader) {
        if (maxItemCount == null) {
            return reader;
        }
        return new CappedReader<>(reader, maxItemCount);
    }

}
