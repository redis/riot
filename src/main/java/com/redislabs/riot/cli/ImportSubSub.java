package com.redislabs.riot.cli;

import java.util.Map;

import org.springframework.batch.item.ItemWriter;

import com.redislabs.riot.RiotApplication.RedisDriver;
import com.redislabs.riot.redis.RedisConverter;
import com.redislabs.riot.redis.writer.JedisPipelineWriter;
import com.redislabs.riot.redis.writer.LettuceWriter;
import com.redislabs.riot.redis.writer.RedisItemWriter;

import lombok.Getter;
import picocli.CommandLine.Option;
import picocli.CommandLine.ParentCommand;

public abstract class ImportSubSub implements Runnable {

	@ParentCommand
	@Getter
	private ImportSub parent;
	@Option(names = "--key-separator", description = "Redis key separator. (default: ${DEFAULT-VALUE}).")
	private String separator = ":";

	protected RedisDriver getDriver() {
		return parent.getParent().getParent().getDriver();
	}

	@Override
	public void run() {
		parent.execute(writer(), getTargetDescription());
	}

	private ItemWriter<Map<String, Object>> writer() {
		switch (getDriver()) {
		case Lettuce:
			return lettuceWriter();
		default:
			return jedisWriter();
		}
	}

	private LettuceWriter lettuceWriter() {
		LettuceWriter writer = new LettuceWriter();
		writer.setPool(parent.getParent().getParent().redisConnectionBuilder().buildLettucePool());
		writer.setItemWriter(redisWriter());
		return writer;
	}

	protected ItemWriter<Map<String, Object>> jedisWriter() {
		JedisPipelineWriter writer = new JedisPipelineWriter();
		writer.setPool(parent.getParent().getParent().redisConnectionBuilder().buildJedisPool());
		writer.setItemWriter(redisWriter());
		return writer;
	}

	private RedisItemWriter redisWriter() {
		CommandsBuilder commandsBuilder = new CommandsBuilder();
		commandsBuilder.setDriver(getDriver());
		RedisItemWriter writer = redisItemWriter();
		writer.setConverter(redisConverter());
		writer.setCommands(commandsBuilder.build());
		return writer;
	}

	protected abstract RedisItemWriter redisItemWriter();

	public String getTargetDescription() {
		return getDataStructure() + " \"" + getKeyspaceDescription() + "\"";
	}

	protected abstract String getDataStructure();

	protected String getKeyspaceDescription() {
		String description = getKeyspace() == null ? "" : getKeyspace();
		for (String key : getKeys()) {
			description += separator + "<" + key + ">";
		}
		return description;
	}

	protected RedisConverter redisConverter() {
		RedisConverter converter = new RedisConverter();
		converter.setKeyspace(getKeyspace());
		converter.setKeys(getKeys());
		converter.setSeparator(separator);
		return converter;
	}

	protected abstract String getKeyspace();

	protected abstract String[] getKeys();

}
