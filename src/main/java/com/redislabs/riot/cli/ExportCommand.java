package com.redislabs.riot.cli;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;

import com.redislabs.riot.cli.redis.RediSearchReaderOptions;
import com.redislabs.riot.cli.redis.RedisReaderOptions;

import picocli.CommandLine.ArgGroup;

public abstract class ExportCommand extends TransferCommand {

	private final Logger log = LoggerFactory.getLogger(ExportCommand.class);

	@ArgGroup(exclusive = false, heading = "Redis reader options%n")
	private RedisReaderOptions redisReader = new RedisReaderOptions();

	@ArgGroup(exclusive = false, heading = "RediSearch reader options%n")
	private RediSearchReaderOptions searchReader = new RediSearchReaderOptions();

	private ItemReader<Map<String, Object>> reader() {
		if (searchReader.isSet()) {
			return searchReader.reader(redis().rediSearchClient());
		}
		return redisReader.reader(redis().jedisPool());
	}

	@Override
	public void run() {
		ItemWriter<Map<String, Object>> writer;
		try {
			writer = writer();
		} catch (Exception e) {
			log.error("Could not initialize writer", e);
			return;
		}
		transfer(reader(), writer);
	}

	protected abstract ItemWriter<Map<String, Object>> writer() throws Exception;

}
