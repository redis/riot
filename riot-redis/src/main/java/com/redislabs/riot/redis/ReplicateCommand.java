package com.redislabs.riot.redis;

import java.util.Collections;
import java.util.List;

import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.redis.RedisKeyDumpItemReader;
import org.springframework.batch.item.redis.RedisKeyDumpItemWriter;
import org.springframework.batch.item.redis.support.KeyDump;

import com.redislabs.riot.AbstractFlushingTransferCommand;
import com.redislabs.riot.RedisConnectionOptions;
import com.redislabs.riot.RedisExportOptions;

import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Option;

@Command(name = "replicate", aliases = {
		"r" }, description = "Replicate a source Redis database in a target Redis database")
public class ReplicateCommand extends AbstractFlushingTransferCommand<KeyDump<String>, KeyDump<String>> {

	@Mixin
	private RedisConnectionOptions targetRedis = new RedisConnectionOptions();
	@Mixin
	private RedisExportOptions options = new RedisExportOptions();
	@Option(names = "--live", description = "Enable live replication")
	private boolean live;

	@Override
	protected Long flushPeriod() {
		if (live) {
			return super.flushPeriod();
		}
		return null;
	}

	@Override
	protected String taskName() {
		return "Replicating from";
	}

	@Override
	protected List<ItemReader<KeyDump<String>>> readers() throws Exception {
		RedisKeyDumpItemReader<String> reader = configure(RedisKeyDumpItemReader.builder()
				.scanCount(options.getScanCount()).scanMatch(options.getScanMatch()).batch(options.getBatchSize())
				.threads(options.getThreads()).queueCapacity(options.getQueueCapacity()).live(live)).build();
		reader.setName(String.valueOf(getRedisURI()));
		return Collections.singletonList(reader);
	}

	@Override
	protected RedisKeyDumpItemWriter<String, String> writer() {
		RedisKeyDumpItemWriter<String, String> writer = configure(RedisKeyDumpItemWriter.builder().replace(true),
				targetRedis).build();
		writer.setName(String.valueOf(targetRedis.getRedisURI()));
		return writer;
	}

	@Override
	protected ItemProcessor<KeyDump<String>, KeyDump<String>> processor() {
		return null;
	}

}
