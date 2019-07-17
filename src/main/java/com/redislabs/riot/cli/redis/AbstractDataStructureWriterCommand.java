package com.redislabs.riot.cli.redis;

import com.redislabs.riot.cli.AbstractCommand;
import com.redislabs.riot.cli.AbstractRedisWriterCommand;
import com.redislabs.riot.cli.RedisKeyOptions;
import com.redislabs.riot.redis.RedisConverter;
import com.redislabs.riot.redis.writer.AbstractRedisItemWriter;

import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.ParentCommand;
import picocli.CommandLine.Spec;

public abstract class AbstractDataStructureWriterCommand extends AbstractCommand {

	@Spec
	protected CommandSpec commandSpec;

	@ParentCommand
	private AbstractRedisWriterCommand<?> parent;

	@ArgGroup(exclusive = false, heading = "Redis keyspace%n")
	private RedisKeyOptions key = new RedisKeyOptions();

	protected RedisConverter redisConverter() {
		return new RedisConverter(key.getSeparator(), key.getSpace(), key.getFields());
	}

	private String keyspaceDescription() {
		if (key.getSpace() == null) {
			return keysDescription();
		}
		if (key.getFields().length > 0) {
			return key.getSpace() + key.getSeparator() + keysDescription();
		}
		return key.getSpace();
	}

	private String keysDescription() {
		return String.join(key.getSeparator(), wrap(key.getFields()));
	}

	private String[] wrap(String[] fields) {
		String[] results = new String[fields.length];
		for (int index = 0; index < fields.length; index++) {
			results[index] = "<" + fields[index] + ">";
		}
		return results;
	}

	protected String description() {
		return "Redis " + commandSpec.name() + " \"" + keyspaceDescription() + "\"";
	}

	@Override
	public void run() {
		AbstractRedisItemWriter writer = writer();
		writer.setConverter(redisConverter());
		parent.execute(writer, description());
	}

	protected abstract AbstractRedisItemWriter writer();

}
