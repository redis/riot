package com.redislabs.riot.redis;

import com.redislabs.riot.AbstractTransferCommand;
import com.redislabs.riot.RedisConnectionOptions;
import com.redislabs.riot.RedisExportOptions;
import com.redislabs.riot.Transfer;
import io.lettuce.core.RedisURI;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.redis.RedisKeyDumpItemReader;
import org.springframework.batch.item.redis.RedisKeyDumpItemWriter;
import org.springframework.batch.item.redis.support.KeyDump;
import org.springframework.batch.item.support.PassThroughItemProcessor;
import picocli.CommandLine;
import picocli.CommandLine.Command;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

@Command(name = "replicate", aliases = {"r"}, description = "Replicate a source Redis database in a target Redis database")
public class ReplicateCommand extends AbstractTransferCommand<KeyDump<String>, KeyDump<String>> {

    @CommandLine.Mixin
    private RedisConnectionOptions targetRedis = new RedisConnectionOptions();
    @CommandLine.Mixin
    private RedisExportOptions options = new RedisExportOptions();
    @CommandLine.Option(names = "--live", description = "Enable live replication")
    private boolean live;
    @CommandLine.Option(names = "--flush-interval", description = "Duration between notification flushes (default: ${DEFAULT-VALUE})", paramLabel = "<ms>")
    private long flushPeriod = 50;

    @Override
    @SuppressWarnings({"rawtypes", "unchecked"})
    public List<Transfer<KeyDump<String>, KeyDump<String>>> transfers() {
        RedisKeyDumpItemReader<String> reader = configure(RedisKeyDumpItemReader.builder().scanCount(options.getScanCount()).scanMatch(options.getScanMatch()).batch(options.getBatchSize()).threads(options.getThreads()).queueCapacity(options.getQueueCapacity()).live(live)).build();
        ItemWriter writer = configure(RedisKeyDumpItemWriter.builder().replace(true), targetRedis).build();
        return transfers("Replicating from " + toString(getRedisConnectionOptions().getRedisURI()) + " to " + toString(targetRedis.getRedisURI()), reader, new PassThroughItemProcessor<>(), writer);
    }

    @Override
    protected Long flushPeriod() {
        if (live) {
            return flushPeriod;
        }
        return super.flushPeriod();
    }

}
