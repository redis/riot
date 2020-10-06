package com.redislabs.riot.redis;

import java.util.Collections;
import java.util.List;

import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.redis.RedisKeyDumpItemReader;
import org.springframework.batch.item.redis.RedisKeyDumpItemWriter;
import org.springframework.batch.item.redis.support.KeyDump;

import com.redislabs.riot.AbstractTransferCommand;
import com.redislabs.riot.RedisConnectionOptions;
import com.redislabs.riot.RedisExportOptions;

import picocli.CommandLine;
import picocli.CommandLine.Command;

@Command(name = "replicate", aliases = {
		"r" }, description = "Replicate a source Redis database in a target Redis database")
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
	protected List<RedisKeyDumpItemReader<String>> readers() throws Exception {
		return Collections.singletonList(reader());
	}

	private RedisKeyDumpItemReader<String> reader() {
		RedisKeyDumpItemReader<String> reader = getApp().configure(RedisKeyDumpItemReader.builder()
				.scanCount(options.getScanCount()).scanMatch(options.getScanMatch()).batch(options.getBatchSize())
				.threads(options.getThreads()).queueCapacity(options.getQueueCapacity()).live(live)).build();
		reader.setName(String.valueOf(getApp().getRedisConnectionOptions().getRedisURI()));
		return reader;
	}

	@Override
	protected RedisKeyDumpItemWriter<String, String> writer() {
		RedisKeyDumpItemWriter<String, String> writer = getApp()
				.configure(RedisKeyDumpItemWriter.builder().replace(true), targetRedis).build();
		writer.setName(String.valueOf(targetRedis.getRedisURI()));
		return writer;
	}

	@Override
	protected ItemProcessor<KeyDump<String>, KeyDump<String>> processor() {
		return null;
	}

	@Override
	protected String taskName() {
		return "Replicating";
	}

	@Override
	protected Long flushPeriod() {
		if (live) {
			return flushPeriod;
		}
		return super.flushPeriod();
	}

}
