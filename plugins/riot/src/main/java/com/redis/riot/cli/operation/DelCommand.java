package com.redis.riot.cli.operation;

import java.util.Map;

import com.redis.spring.batch.writer.operation.Del;

import picocli.CommandLine.Command;

@Command(name = "del", description = "Delete keys")
public class DelCommand extends AbstractKeyCommand {

	@Override
	public Del<String, String, Map<String, Object>> operation() {
		return new Del<>(key());
	}

}
