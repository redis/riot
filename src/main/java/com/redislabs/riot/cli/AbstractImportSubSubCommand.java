package com.redislabs.riot.cli;

import java.util.Map;
import java.util.concurrent.Callable;

import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.item.ItemStreamWriter;

import com.redislabs.riot.RiotApplication.RedisDriver;
import com.redislabs.riot.redis.RedisConverter;
import com.redislabs.riot.redis.writer.AbstractRedisItemWriter;
import com.redislabs.riot.redis.writer.JedisCommands;
import com.redislabs.riot.redis.writer.JedisPipelineWriter;
import com.redislabs.riot.redis.writer.LettuceCommands;
import com.redislabs.riot.redis.writer.LettuceWriter;
import com.redislabs.riot.redis.writer.RedisCommands;

import picocli.CommandLine.ParentCommand;

public abstract class AbstractImportSubSubCommand implements Callable<ExitStatus> {

	@ParentCommand
	protected ImportSubCommand parent;

	@Override
	public ExitStatus call() throws Exception {
		return parent.call(writer());
	}

	private ItemStreamWriter<Map<String, Object>> writer() {
		switch (getDriver()) {
		case Lettuce:
			return lettuceWriter();
		default:
			return jedisWriter();
		}
	}

	protected RedisDriver getDriver() {
		return parent.getParent().getParent().getDriver();
	}

	protected ItemStreamWriter<Map<String, Object>> jedisWriter() {
		JedisPipelineWriter writer = new JedisPipelineWriter();
		writer.setPool(parent.getParent().getParent().redisConnectionBuilder().buildJedisPool());
		writer.setItemWriter(redisWriter());
		return writer;
	}

	private AbstractRedisItemWriter redisWriter() {
		AbstractRedisItemWriter writer = redisItemWriter();
		writer.setConverter(redisConverter());
		writer.setCommands(redisCommands());
		return writer;
	}

	protected abstract AbstractRedisItemWriter redisItemWriter();

	protected LettuceWriter lettuceWriter() {
		LettuceWriter writer = new LettuceWriter();
		writer.setPool(parent.getParent().getParent().redisConnectionBuilder().buildLettucePool());
		writer.setItemWriter(redisWriter());
		return writer;
	}

	protected RedisCommands redisCommands() {
		switch (getDriver()) {
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

	public String getTargetDescription() {
		return getDataStructure() + " \"" + getKeyspaceDescription() + "\"";
	}

	protected abstract String getDataStructure();

	protected String getKeyspaceDescription() {
		String description = getKeyspace() == null ? "" : getKeyspace();
		for (String key : getKeys()) {
			description += ":<" + key + ">";
		}
		return description;
	}

	protected RedisConverter redisConverter() {
		RedisConverter converter = new RedisConverter();
		converter.setKeyspace(getKeyspace());
		converter.setKeys(getKeys());
		return converter;
	}

	protected abstract String getKeyspace();

	protected abstract String[] getKeys();

}
