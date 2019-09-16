package com.redislabs.riot.cli;

import java.util.Map;

import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;

import com.redislabs.riot.cli.redis.RediSearchReaderOptions;
import com.redislabs.riot.cli.redis.RedisReaderOptions;

import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;

@Command
public abstract class ExportCommand extends TransferCommand<Map<String, Object>, Map<String, Object>> {

	@ArgGroup(exclusive = false, heading = "Redis reader options%n")
	private RedisReaderOptions redisReader = new RedisReaderOptions();

	@ArgGroup(exclusive = false, heading = "RediSearch reader options%n")
	private RediSearchReaderOptions searchReader = new RediSearchReaderOptions();

	@Override
	protected ItemReader<Map<String, Object>> reader() {
		if (searchReader.isSet()) {
			return searchReader.reader(getRedisOptions().rediSearchClient());
		}
		return redisReader.reader(getRedisOptions().jedisPool());
	}

	@Override
	protected ItemProcessor<Map<String, Object>, Map<String, Object>> processor() throws Exception {
		return null;
	}

}
