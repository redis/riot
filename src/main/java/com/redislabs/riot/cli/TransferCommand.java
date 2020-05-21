package com.redislabs.riot.cli;

import com.redislabs.picocliredis.RedisCommandLineOptions;
import com.redislabs.riot.transfer.ErrorHandler;
import com.redislabs.riot.transfer.Transfer;
import com.redislabs.riot.transfer.TransferExecution;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.item.redis.support.RedisOptions;
import org.springframework.batch.item.redisearch.RediSearchOptions;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.util.concurrent.TimeUnit;

@Slf4j
@Command
public abstract class TransferCommand<I, O> extends RiotCommand {

    @Option(names = "--threads", description = "Thread count (default: ${DEFAULT-VALUE})", paramLabel = "<count>")
    private int threads = 1;
    @Option(names = {"-b", "--batch"}, description = "Number of items in each batch (default: ${DEFAULT-VALUE})", paramLabel = "<size>")
    private int batchSize = 50;
    @Option(names = {"-m", "--max"}, description = "Max number of items to read", paramLabel = "<count>")
    private Integer maxItemCount;
    @Option(names = "--sleep", description = "Sleep duration in millis between reads", paramLabel = "<ms>")
    private Long sleep;
    @Option(names = "--progress", description = "Progress reporting interval (default: ${DEFAULT-VALUE} ms)", paramLabel = "<ms>")
    private long progressRate = 300;
    @Option(names = "--max-wait", description = "Max duration to wait for transfer to complete", paramLabel = "<ms>")
    private Long maxWait;
    @Option(names = "--pool-size", description = "Max connections in pool (default: #threads)", paramLabel = "<int>")
    private Integer maxTotal;

    protected int getPoolMaxTotal() {
        if (maxTotal == null) {
            return threads;
        }
        return maxTotal;
    }

    @Override
    public void run() {
        Transfer<I, O> transfer = null;
        try {
            transfer = getTransfer();
        } catch (Exception e) {
            log.error("Could not initialize transfer", e);
            return;
        }
        TransferExecution.Options options = TransferExecution.Options.builder().batchSize(batchSize).errorHandler(errorHandler()).maxItemCount(maxItemCount).nThreads(threads).build();
        TransferExecution<I, O> execution = transfer.execute(options);
        if (!isQuiet()) {
            transfer.addListener(ProgressBarReporter.builder().taskName(taskName()).initialMax(maxItemCount==null?null:maxItemCount.longValue()).period(progressRate).timeUnit(TimeUnit.MILLISECONDS).metricsProvider(execution).build());
        }
        execution.awaitTermination(maxWait(), TimeUnit.MILLISECONDS);
    }

    protected abstract Transfer<I, O> getTransfer() throws Exception;

    protected abstract String taskName();

    protected RedisOptions redisOptions() {
        return redisOptions(getOptions());
    }

    protected RediSearchOptions rediSearchOptions() {
        return RediSearchOptions.builder().redisURI(getOptions().getRedisURI()).clientOptions(getOptions().clientOptions()).clientResources(getOptions().clientResources()).poolOptions(RediSearchOptions.PoolOptions.builder().maxTotal(getPoolMaxTotal()).build()).build();
    }

    protected RedisOptions redisOptions(RedisCommandLineOptions cliOptions) {
        return RedisOptions.builder().redisURI(cliOptions.getRedisURI()).clientOptions(cliOptions.clientOptions()).clientResources(cliOptions.clientResources()).poolOptions(RedisOptions.PoolOptions.builder().maxTotal(getPoolMaxTotal()).build()).cluster(cliOptions.isCluster()).clusterClientOptions(cliOptions.clusterClientOptions()).build();
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

}
