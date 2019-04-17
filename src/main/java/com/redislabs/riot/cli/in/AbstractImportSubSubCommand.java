package com.redislabs.riot.cli.in;

import java.util.Map;

import org.springframework.batch.item.ItemStreamWriter;

import com.redislabs.riot.cli.AbstractSubSubCommand;
import com.redislabs.riot.redis.writer.AbstractRedisWriter;

import picocli.CommandLine.ParentCommand;

public abstract class AbstractImportSubSubCommand
		extends AbstractSubSubCommand<Map<String, Object>, Map<String, Object>> {

	@ParentCommand
	private AbstractImportSubCommand parent;

	@Override
	protected ItemStreamWriter<Map<String, Object>> writer() {
		AbstractRedisWriter writer = redisWriter();
		writer.setRedisClient(parent.getParent().redisConnectionBuilder().buildClient());
		return writer;
	}

	protected abstract AbstractRedisWriter redisWriter();

}
