package com.redislabs.riot.cli.in;

import java.util.Map;

import org.springframework.batch.item.ItemWriter;

import com.redislabs.lettusearch.RediSearchClient;
import com.redislabs.riot.redis.writer.AbstractRedisDataStructureItemWriter;
import com.redislabs.riot.redis.writer.JedisWriter;
import com.redislabs.riot.redis.writer.LettuceAsyncWriter;
import com.redislabs.riot.redis.writer.LettuceReactiveWriter;

public abstract class AbstractRedisImportWriterCommand extends AbstractImportWriterCommand {

	@Override
	protected ItemWriter<Map<String, Object>> writer() {
		AbstractRedisDataStructureItemWriter itemWriter = redisItemWriter();
		itemWriter.setConverter(redisConverter());
		switch (getRoot().getDriver()) {
		case LettuceAsync:
			return new LettuceAsyncWriter(getRoot().lettucePool(), itemWriter);
		case LettuceReactive:
			RediSearchClient client = getRoot().lettuceClient();
			return new LettuceReactiveWriter(client.connect(), itemWriter);
		default:
			return new JedisWriter(getRoot().jedisPool(), itemWriter);
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
