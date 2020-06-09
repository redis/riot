package com.redislabs.riot.cli;

import com.redislabs.lettusearch.RediSearchClient;
import com.redislabs.lettusearch.StatefulRediSearchConnection;
import com.redislabs.picocliredis.RedisOptions;
import com.redislabs.riot.Transfer;
import com.redislabs.riot.cli.progress.ProgressBarOptions;
import com.redislabs.riot.cli.progress.ProgressBarReporter;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.support.ConnectionPoolSupport;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import picocli.CommandLine;

public class TransferCommand extends RiotCommand {

    @CommandLine.Option(names = "--threads", description = "Thread count (default: ${DEFAULT-VALUE})", paramLabel = "<count>")
    private int threads = 1;
    @CommandLine.Option(names = {"-b", "--batch"}, description = "Number of items in each batch (default: ${DEFAULT-VALUE})", paramLabel = "<size>")
    private int batchSize = 50;
    @CommandLine.Option(names = {"-m", "--max"}, description = "Max number of items to read", paramLabel = "<count>")
    private Integer maxItemCount;
    @CommandLine.Option(names = "--pool-size", description = "Max connections in pool (default: #threads)", paramLabel = "<int>")
    private Integer poolSize;

    public int getPoolSize() {
        if (poolSize == null) {
            return threads;
        }
        return poolSize;
    }

    protected GenericObjectPool<StatefulRedisConnection<String, String>> connectionPool(RedisClient redisClient) {
        return ConnectionPoolSupport.createGenericObjectPool(redisClient::connect, poolConfig());
    }

    protected GenericObjectPool<StatefulRedisClusterConnection<String, String>> connectionPool(RedisClusterClient redisClusterClient) {
        return ConnectionPoolSupport.createGenericObjectPool(redisClusterClient::connect, poolConfig());
    }

    protected GenericObjectPool<StatefulRediSearchConnection<String, String>> connectionPool(RediSearchClient rediSearchClient) {
        return ConnectionPoolSupport.createGenericObjectPool(rediSearchClient::connect, poolConfig());
    }

    protected <C extends StatefulConnection<String, String>> GenericObjectPoolConfig<C> poolConfig() {
        GenericObjectPoolConfig<C> poolConfig = new GenericObjectPoolConfig<>();
        poolConfig.setMaxTotal(getPoolSize());
        return poolConfig;
    }

    public <I, O> void execute(String taskName, ItemReader<I> reader, ItemProcessor<I, O> processor, ItemWriter<O> writer) {
        Transfer<I, O> transfer = Transfer.<I, O>builder().reader(reader).processor(processor).writer(writer).options(transferOptions()).build();
        ProgressBarOptions progressBarOptions = ProgressBarOptions.builder().taskName(taskName).initialMax(maxItemCount).quiet(isQuiet()).build();
        ProgressBarReporter reporter = ProgressBarReporter.builder().transfer(transfer).options(progressBarOptions).build();
        reporter.start();
        transfer.execute();
        reporter.stop();
    }

    protected com.redislabs.riot.TransferOptions transferOptions() {
        return com.redislabs.riot.TransferOptions.builder().batchSize(batchSize).threadCount(threads).maxItemCount(maxItemCount).build();
    }

    protected RediSearchClient rediSearchClient(RedisOptions redisOptions) {
        RediSearchClient client = RediSearchClient.create(redisOptions.clientResources(), redisOptions.getRedisURI());
        client.setOptions(redisOptions.clientOptions());
        return client;
    }

}
