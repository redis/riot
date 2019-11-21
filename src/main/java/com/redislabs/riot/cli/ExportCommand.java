package com.redislabs.riot.cli;

import java.util.Map;

import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;

import com.redislabs.picocliredis.RedisOptions;
import com.redislabs.riot.cli.redis.RediSearchReaderOptions;
import com.redislabs.riot.cli.redis.RedisReaderOptions;

import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;

@Command
public abstract class ExportCommand extends TransferCommand {

	@ArgGroup(exclusive = false, heading = "Redis reader options%n", order = 4)
	private RedisReaderOptions redisReader = new RedisReaderOptions();
	@ArgGroup(exclusive = false, heading = "RediSearch reader options%n", order = 5)
	private RediSearchReaderOptions searchReader = new RediSearchReaderOptions();

	@SuppressWarnings("rawtypes")
	@Override
	protected ItemReader reader(RedisOptions redisOptions) {
		if (searchReader.isSet()) {
			return searchReader.reader(redisOptions.lettuSearchClient());
		}
		return redisReader.reader(redisOptions.jedisPool());
	}

	@Override
	protected ItemProcessor<Map<String, Object>, Map<String, Object>> processor(RedisOptions options) throws Exception {
		return null;
	}

}
