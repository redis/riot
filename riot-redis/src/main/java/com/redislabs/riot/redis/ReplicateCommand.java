package com.redislabs.riot.redis;

import java.util.Collections;
import java.util.List;

import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.redis.RedisDumpItemReader;
import org.springframework.batch.item.redis.RedisDumpItemWriter;
import org.springframework.batch.item.redis.support.KeyValue;

import com.redislabs.riot.AbstractFlushingTransferCommand;
import com.redislabs.riot.RedisConnectionOptions;
import com.redislabs.riot.RedisExportOptions;

import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Option;

@Command(name = "replicate", aliases = {
	"r" }, description = "Replicate a source Redis database in a target Redis database")
public class ReplicateCommand
	extends AbstractFlushingTransferCommand<KeyValue<String, byte[]>, KeyValue<String, byte[]>> {

    @Mixin
    private RedisConnectionOptions targetRedis = new RedisConnectionOptions();
    @Mixin
    private RedisExportOptions options = new RedisExportOptions();
    @Option(names = "--live", description = "Enable live replication")
    private boolean live;

    @Override
    protected boolean flushingEnabled() {
	return live;
    }

    @Override
    protected String taskName() {
	return "Replicating from";
    }

    @Override
    protected List<ItemReader<KeyValue<String, byte[]>>> readers() throws Exception {
	RedisDumpItemReader<String> reader = configure(RedisDumpItemReader.builder().scanCount(options.getScanCount())
		.scanMatch(options.getScanMatch()).batch(options.getReaderBatchSize())
		.threads(options.getReaderThreads()).queueCapacity(options.getQueueCapacity()).live(live)).build();
	reader.setName(toString(getRedisConnectionOptions().redisURI()));
	return Collections.singletonList(reader);
    }

    @Override
    protected RedisDumpItemWriter<String, String> writer() throws Exception {
	RedisDumpItemWriter<String, String> writer = configure(RedisDumpItemWriter.builder().replace(true), targetRedis)
		.build();
	writer.setName(toString(targetRedis.redisURI()));
	return writer;
    }

    @Override
    protected ItemProcessor<KeyValue<String, byte[]>, KeyValue<String, byte[]>> processor() {
	return null;
    }

}
