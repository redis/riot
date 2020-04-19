package com.redislabs.riot.cli;

import com.redislabs.picocliredis.RedisOptions;
import com.redislabs.riot.redis.*;
import com.redislabs.riot.redis.writer.*;
import com.redislabs.riot.transfer.*;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.util.concurrent.TimeUnit;

@Slf4j
@Command
public abstract class TransferCommand<I, O> extends RiotCommand {

    @Option(names = "--threads", description = "Thread count (default: ${DEFAULT-VALUE})", paramLabel = "<count>")
    private int threads = 1;
    @Option(names = {"-b",
            "--batch"}, description = "Number of items in each batch (default: ${DEFAULT-VALUE})", paramLabel = "<size>")
    private int batchSize = 50;
    @Option(names = {"-m", "--max"}, description = "Max number of items to read", paramLabel = "<count>")
    private Long maxItemCount;
    @Option(names = "--sleep", description = "Sleep duration in millis between reads", paramLabel = "<ms>")
    private Long sleep;
    @Option(names = "--progress", description = "Progress reporting interval (default: ${DEFAULT-VALUE} ms)", paramLabel = "<ms>")
    private long progressRate = 300;
    @Option(names = "--max-wait", description = "Max duration to wait for transfer to complete", paramLabel = "<ms>")
    private Long maxWait;

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
        execute(transfer(getMainFlowName(), reader, processor, writer));
    }

    protected abstract String getMainFlowName();

    protected abstract ItemReader<I> reader() throws Exception;

    protected abstract ItemProcessor<I, O> processor() throws Exception;

    protected abstract ItemWriter<O> writer() throws Exception;

    protected Transfer<I, O> transfer(String name, ItemReader<I> reader, ItemProcessor<I, O> processor, ItemWriter<O> writer) {
        return Transfer.<I, O>builder().flow(flow(name, reader, processor, writer)).build();
    }

    protected ErrorHandler errorHandler() {
        return e -> log.error("Could not read item", e);
    }

    protected Flow<I, O> flow(String name, ItemReader<I> reader, ItemProcessor<I, O> processor, ItemWriter<O> writer) {
        return Flow.<I, O>builder().name(name).batchSize(batchSize).nThreads(threads).reader(throttle(cap(reader)))
                .processor(processor).writer(writer).errorHandler(errorHandler()).build();
    }

    protected void execute(Transfer<I, O> transfer) {
        TransferExecution<I, O> execution = transfer.execute();
        if (!isQuiet()) {
            transfer.addListener(ProgressBarReporter.builder().taskName(taskName()).initialMax(maxItemCount).period(progressRate).timeUnit(TimeUnit.MILLISECONDS).metricsProvider(execution).build());
        }
        execution.awaitTermination(maxWait(), TimeUnit.MILLISECONDS);
    }

    private long maxWait() {
        if (maxWait == null) {
            return Long.MAX_VALUE;
        }
        return maxWait;
    }

    private ItemReader<I> throttle(ItemReader<I> reader) {
        if (sleep == null) {
            return reader;
        }
        return ThrottledReader.<I>builder().reader(reader).sleep(sleep).build();
    }

    private ItemReader<I> cap(ItemReader<I> reader) {
        if (maxItemCount == null) {
            return reader;
        }
        return new CappedReader<>(reader, maxItemCount);
    }

    protected abstract String taskName();

    protected AbstractRedisItemWriter<O> writer(RedisOptions redisOptions, CommandWriter<O> writer) {
        if (writer instanceof AbstractCommandWriter) {
            ((AbstractCommandWriter<O>) writer).setCommands(redisCommands(redisOptions));
        }
        if (redisOptions.isJedis()) {
            if (redisOptions.isCluster()) {
                return JedisClusterWriter.<O>builder().cluster(redisOptions.jedisCluster()).writer(writer).build();
            }
            return JedisPipelineWriter.<O>builder().pool(redisOptions.jedisPool()).writer(writer).build();
        }
        AbstractLettuceItemWriter<O> lettuceWriter = lettuceWriter(redisOptions);
        lettuceWriter.setWriter(writer);
        if (writer instanceof RediSearchCommandWriter) {
            lettuceWriter.setApi(redisOptions.lettuSearchApi());
            lettuceWriter.setPool(redisOptions.pool(redisOptions.rediSearchClient()));
        } else {
            lettuceWriter.setApi(redisOptions.lettuceApi());
            lettuceWriter.setPool(redisOptions.pool(redisOptions.lettuceClient()));
        }
        return lettuceWriter;
    }

    private AbstractLettuceItemWriter<O> lettuceWriter(RedisOptions redisOptions) {
        switch (redisOptions.getApi()) {
            case REACTIVE:
                return LettuceReactiveItemWriter.<O>builder().build();
            case SYNC:
                return LettuceSyncItemWriter.<O>builder().build();
            default:
                return LettuceAsyncItemWriter.<O>builder().timeout(redisOptions.getCommandTimeout())
                        .fireAndForget(redisOptions.isFireAndForget()).build();
        }
    }

    private RedisCommands<?> redisCommands(RedisOptions redis) {
        if (redis.isJedis()) {
            if (redis.isCluster()) {
                return new JedisClusterCommands();
            }
            return new JedisPipelineCommands();
        }
        switch (redis.getApi()) {
            case REACTIVE:
                return new LettuceReactiveCommands();
            case SYNC:
                return new LettuceSyncCommands();
            default:
                return new LettuceAsyncCommands();
        }
    }

}
