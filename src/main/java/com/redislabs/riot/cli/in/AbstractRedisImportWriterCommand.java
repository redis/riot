package com.redislabs.riot.cli.in;

import java.util.Map;

import org.springframework.batch.item.ItemWriter;

import com.redislabs.riot.cli.RedisDriver;
import com.redislabs.riot.redis.writer.AbstractRedisDataStructureItemWriter;
import com.redislabs.riot.redis.writer.JedisWriter;
import com.redislabs.riot.redis.writer.LettuceWriter;

import picocli.CommandLine.Option;

public abstract class AbstractRedisImportWriterCommand extends AbstractImportWriterCommand {

	@Option(names = "--driver", description = "Redis driver: ${COMPLETION-CANDIDATES}. (default: ${DEFAULT-VALUE})")
	private RedisDriver driver = RedisDriver.Jedis;

	@Override
	protected ItemWriter<Map<String, Object>> writer() {
		AbstractRedisDataStructureItemWriter itemWriter = redisItemWriter();
		itemWriter.setConverter(redisConverter());
		switch (driver) {
		case Jedis:
			return new JedisWriter(getRoot().jedisPool(), itemWriter);
		default:
			return new LettuceWriter(getRoot().lettucePool(), itemWriter);
		}
	}

	protected abstract AbstractRedisDataStructureItemWriter redisItemWriter();

	public String getTargetDescription() {
		return getDataStructure() + " \"" + getKeyspaceDescription() + "\"";
	}

	protected abstract String getDataStructure();

	protected String getKeyspaceDescription() {
		String description = getKeyspace() == null ? "" : getKeyspace();
		for (String key : getKeys()) {
			description += getSeparator() + "<" + key + ">";
		}
		return description;
	}

}
