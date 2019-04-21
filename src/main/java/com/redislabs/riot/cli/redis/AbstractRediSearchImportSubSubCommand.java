package com.redislabs.riot.cli.redis;

import java.util.Map;

import org.springframework.batch.item.ItemStreamWriter;

import com.redislabs.riot.redis.writer.AbstractRedisItemWriter;
import com.redislabs.riot.redis.writer.RedisCommands;
import com.redislabs.riot.redis.writer.search.AbstractRediSearchItemWriter;

import lombok.Getter;
import picocli.CommandLine.Option;

public abstract class AbstractRediSearchImportSubSubCommand extends AbstractRedisImportSubSubCommand {

	@Getter
	@Option(names = "--index", description = "Name of the RediSearch index", required = true)
	private String index;

	@Override
	protected ItemStreamWriter<Map<String, Object>> writer() {
		return lettuceWriter();
	}

	@Override
	protected RedisCommands redisCommands() {
		return lettuceCommands();
	}

	@Override
	protected AbstractRedisItemWriter redisItemWriter() {
		AbstractRediSearchItemWriter writer = rediSearchItemWriter();
		writer.setIndex(index);
		return writer;
	}

	protected abstract AbstractRediSearchItemWriter rediSearchItemWriter();

	@Override
	protected String getKeyspaceDescription() {
		return index;
	}

}
