package com.redislabs.riot.cli.redis;

import java.util.Map;

import org.springframework.batch.item.ItemStreamWriter;

import com.redislabs.riot.redis.writer.AbstractRedisItemWriter;
import com.redislabs.riot.redis.writer.RedisCommands;
import com.redislabs.riot.redis.writer.search.AbstractRediSearchItemWriter;

import lombok.Getter;
import picocli.CommandLine.Option;

public abstract class AbstractRediSearchImportSubSubCommand extends AbstractRedisImportSubSubCommand {

	@Option(names = { "--keyspace" }, description = "Document keyspace prefix.")
	private String keyspace;
	@Option(names = "--keys", required = true, arity = "1..*", description = "Document key fields.")
	private String[] keys = new String[0];
	@Getter
	@Option(names = "--index", description = "Name of the RediSearch index", required = true)
	private String index;
	
	@Override
	protected String getKeyspace() {
		return keyspace;
	}
	
	@Override
	protected String[] getKeys() {
		return keys;
	}

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
