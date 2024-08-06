package com.redis.riot.operation;

import java.util.Map;

import com.redis.spring.batch.item.redis.writer.impl.Del;

import picocli.CommandLine.Command;

@Command(name = "del", description = "Delete keys")
public class DelCommand extends AbstractOperationCommand {

	@Override
	public Del<String, String, Map<String, Object>> operation() {
		return new Del<>(keyFunction());
	}

}