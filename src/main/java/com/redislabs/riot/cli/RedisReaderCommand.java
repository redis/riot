package com.redislabs.riot.cli;

import java.util.Map;

import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;

import com.redislabs.riot.redis.RedisReader;

import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "redis", description = "Redis database")
public class RedisReaderCommand extends AbstractReaderCommand {

	@ArgGroup(exclusive = false, heading = "Redis connection%n")
	private RedisConnectionOptions redis = new RedisConnectionOptions();
	@ArgGroup(exclusive = false, heading = "Redis keyspace%n")
	private RedisKeyOptions key = new RedisKeyOptions();
	@Option(names = "--count", description = "Number of elements to return for each scan call.")
	private Integer scanCount;

	@Override
	public AbstractItemCountingItemStreamItemReader<Map<String, Object>> reader() throws Exception {
		RedisReader reader = new RedisReader(redis.jedisPool());
		reader.setCount(scanCount);
		reader.setMatch(getScanPattern());
		reader.setKeys(key.getFields());
		reader.setKeyspace(key.getSpace());
		reader.setSeparator(key.getSeparator());
		return reader;
	}

	private String getScanPattern() {
		if (key.getSpace() == null) {
			return null;
		}
		return key.getSpace() + key.getSeparator() + "*";
	}

	@Override
	public String getSourceDescription() {
		return "redis " + getScanPattern();
	}

}
