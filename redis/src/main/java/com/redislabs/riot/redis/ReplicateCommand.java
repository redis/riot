package com.redislabs.riot.redis;

import com.redislabs.riot.AbstractTransferCommand;
import com.redislabs.riot.RedisConnectionOptions;
import com.redislabs.riot.RedisExportOptions;
import com.redislabs.riot.Transfer;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.redis.RedisKeyDumpItemReader;
import org.springframework.batch.item.redis.RedisKeyDumpItemWriter;
import org.springframework.batch.item.redis.support.KeyDump;
import org.springframework.batch.item.support.PassThroughItemProcessor;
import picocli.CommandLine;
import picocli.CommandLine.Command;

@Command(name = "replicate", aliases = {"r"}, description = "Replicate a Redis database to another Redis database")
public class ReplicateCommand extends AbstractTransferCommand<KeyDump<String>, KeyDump<String>> {

    @CommandLine.ArgGroup(exclusive = false,  heading = "Target Redis connection options%n")
    private RedisConnectionOptions targetRedis = new RedisConnectionOptions();
    @CommandLine.Mixin
    private RedisExportOptions options = new RedisExportOptions();
    @CommandLine.Option(names = "--flush-interval", description = "Duration between notification flushes (default: ${DEFAULT-VALUE})", paramLabel = "<ms>")
    private long flushPeriod = 50;
    @CommandLine.Option(names = "--live", description = "Live replication")
    private boolean live;

    @Override
    protected ItemReader<KeyDump<String>> reader() {
        return configure(RedisKeyDumpItemReader.builder().scanCount(options.getScanCount()).scanMatch(options.getScanMatch()).batch(options.getBatchSize()).threads(options.getThreads()).queueCapacity(options.getQueueCapacity()).live(live)).build();
    }

    @Override
    protected ItemProcessor<KeyDump<String>, KeyDump<String>> processor() {
        return new PassThroughItemProcessor<>();
    }

    @Override
    protected ItemWriter<KeyDump<String>> writer() {
        return configure(RedisKeyDumpItemWriter.builder().replace(true), targetRedis).build();
    }

    @Override
    protected Transfer<KeyDump<String>, KeyDump<String>> transfer(ItemReader<KeyDump<String>> reader, ItemProcessor<KeyDump<String>, KeyDump<String>> processor, ItemWriter<KeyDump<String>> writer) {
        Transfer<KeyDump<String>, KeyDump<String>> transfer = super.transfer(reader, processor, writer);
        if (live) {
            transfer.setFlushPeriod(flushPeriod);
        }
        return transfer;
    }

    @Override
    protected String taskName() {
        return "Replicating";
    }

}
