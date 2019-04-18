package com.redislabs.riot.cli.in;

import java.util.Map;

import org.springframework.batch.item.ItemStreamWriter;

import com.redislabs.riot.cli.AbstractSubSubCommand;
import com.redislabs.riot.redis.writer.AbstractRedisItemWriter;
import com.redislabs.riot.redis.writer.JedisCommands;
import com.redislabs.riot.redis.writer.JedisWriter;
import com.redislabs.riot.redis.writer.LettuceCommands;
import com.redislabs.riot.redis.writer.LettuceWriter;
import com.redislabs.riot.redis.writer.RedisCommands;

import picocli.CommandLine.ParentCommand;

public abstract class AbstractImportSubSubCommand
		extends AbstractSubSubCommand<Map<String, Object>, Map<String, Object>> {

	@ParentCommand
	private AbstractImportSubCommand parent;

	@Override
	protected ItemStreamWriter<Map<String, Object>> writer() {
		switch (parent.getParent().getDriver()) {
		case Lettuce:
			return lettuceWriter();
		default:
			return jedisWriter();
		}
	}

	private JedisWriter jedisWriter() {
		JedisWriter writer = new JedisWriter();
		writer.setPool(parent.getParent().redisConnectionBuilder().buildJedisPool());
		writer.setItemWriter(redisItemWriter());
		return writer;
	}

	private AbstractRedisItemWriter redisItemWriter() {
		AbstractRedisItemWriter itemWriter = itemWriter();
		itemWriter.setCommands(redisCommands());
		return itemWriter;
	}

	private LettuceWriter lettuceWriter() {
		LettuceWriter writer = new LettuceWriter();
		writer.setPool(parent.getParent().redisConnectionBuilder().buildLettucePool());
		writer.setItemWriter(redisItemWriter());
		return writer;
	}

	private RedisCommands redisCommands() {
		switch (parent.getParent().getDriver()) {
		case Lettuce:
			return new LettuceCommands();
		default:
			return new JedisCommands();
		}
	}

	protected abstract AbstractRedisItemWriter itemWriter();

}
