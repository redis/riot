package com.redislabs.riot.cli.in;

import java.util.Map;

import org.springframework.batch.item.ItemStreamWriter;

import com.redislabs.riot.cli.AbstractSubSubCommand;
import com.redislabs.riot.redis.writer.AbstractRedisItemWriter;
import com.redislabs.riot.redis.writer.JedisCommands;
import com.redislabs.riot.redis.writer.JedisPipelineWriter;
import com.redislabs.riot.redis.writer.LettuceCommands;
import com.redislabs.riot.redis.writer.LettuceWriter;
import com.redislabs.riot.redis.writer.RedisCommands;

import picocli.CommandLine.ParentCommand;

public abstract class AbstractImportSubSubCommand
		extends AbstractSubSubCommand<Map<String, Object>, Map<String, Object>> {

	@ParentCommand
	protected AbstractImportSubCommand parent;

	@Override
	protected ItemStreamWriter<Map<String, Object>> writer() {
		switch (parent.getParent().getDriver()) {
		case Lettuce:
			return lettuceWriter();
		default:
			return jedisWriter();
		}
	}

	protected ItemStreamWriter<Map<String, Object>> jedisWriter() {
		JedisPipelineWriter writer = new JedisPipelineWriter();
		writer.setPool(parent.getParent().redisConnectionBuilder().buildJedisPool());
		writer.setItemWriter(redisItemWriter());
		return writer;
	}

	private AbstractRedisItemWriter redisItemWriter() {
		AbstractRedisItemWriter itemWriter = itemWriter();
		itemWriter.setCommands(redisCommands());
		return itemWriter;
	}

	protected LettuceWriter lettuceWriter() {
		LettuceWriter writer = new LettuceWriter();
		writer.setPool(parent.getParent().redisConnectionBuilder().buildLettucePool());
		writer.setItemWriter(redisItemWriter());
		return writer;
	}

	protected RedisCommands redisCommands() {
		switch (parent.getParent().getDriver()) {
		case Lettuce:
			return lettuceCommands();
		default:
			return jedisCommands();
		}
	}

	private RedisCommands jedisCommands() {
		return new JedisCommands();
	}

	protected RedisCommands lettuceCommands() {
		return new LettuceCommands();
	}

	protected abstract AbstractRedisItemWriter itemWriter();

}
