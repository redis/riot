package com.redislabs.riot.cli;

import com.redislabs.lettuce.helper.RedisOptions;
import com.redislabs.picocliredis.RedisCommandLineOptions;
import com.redislabs.riot.Riot;
import com.redislabs.riot.TransferOptions;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.redis.KeyDump;
import org.springframework.batch.item.redis.RedisKeyDumpItemReader;
import org.springframework.batch.item.redis.RedisKeyDumpItemWriter;
import org.springframework.batch.item.redis.support.QueueOptions;
import org.springframework.batch.item.redis.support.ReaderOptions;
import org.springframework.batch.item.support.PassThroughItemProcessor;
import picocli.CommandLine;
import picocli.CommandLine.Command;

@Command(name = "replicate", description = "Replicate a Redis database to another Redis database")
public class ReplicateCommand extends TransferCommand {

    @CommandLine.ParentCommand
    private Riot riot;
    @CommandLine.Mixin
    private RedisCommandLineOptions target = new RedisCommandLineOptions();
    @CommandLine.Option(names = "--target-pool", description = "Max number of connections in target pool", paramLabel = "<int>")
    private Integer targetPoolMaxTotal;
    @CommandLine.Mixin
    private ExportOptions options = new ExportOptions();
    @CommandLine.Option(names = "--flush-interval", description = "Duration between notification flushes (default: ${DEFAULT-VALUE})", paramLabel = "<ms>")
    private long flushPeriod = 50;
    @CommandLine.Option(names = "--live", description = "Live replication")
    private boolean live;
    @CommandLine.Option(names = "--notification-queue", description = "Keyspace notification queue capacity (default: ${DEFAULT-VALUE})", paramLabel = "<int>", hidden = true)
    private int notificationQueueCapacity = 10000;

    private RedisOptions targetRedisOptions() {
        RedisOptions redisOptions = Riot.redisOptions(target);
        redisOptions.getPoolOptions().setMaxTotal(targetPoolMaxTotal == null ? getPoolMaxTotal() : targetPoolMaxTotal);
        return redisOptions;
    }

    @Override
    protected boolean isQuiet() {
        return riot.isQuiet();
    }

    @Override
    protected TransferOptions transferOptions() {
        TransferOptions transferOptions = super.transferOptions();
        if (live) {
            transferOptions.setFlushPeriod(flushPeriod);
        }
        return transferOptions;
    }

    private RedisKeyDumpItemReader<String, String> reader() {
        ReaderOptions readerOptions = ReaderOptions.builder().scanCount(options.getScanCount()).scanMatch(options.getScanMatch()).batchSize(options.getBatchSize()).threadCount(options.getThreads()).valueQueueOptions(QueueOptions.builder().capacity(options.getQueueCapacity()).build()).live(live).keyspaceNotificationQueueOptions(QueueOptions.builder().capacity(notificationQueueCapacity).build()).build();
        return RedisKeyDumpItemReader.builder().redisOptions(redisOptions()).readerOptions(readerOptions).build();
    }

    @Override
    protected String taskName() {
        return "Replicating";
    }

    @Override
    public void run() {
        execute(reader(), new PassThroughItemProcessor<>(), writer());
    }

    private ItemWriter<KeyDump<String>> writer() {
        return RedisKeyDumpItemWriter.builder().redisOptions(targetRedisOptions()).replace(true).build();
    }

}
