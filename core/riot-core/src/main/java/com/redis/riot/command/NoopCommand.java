package com.redis.riot.command;

import java.util.Map;

import com.redis.riot.HelpOptions;
import com.redis.riot.OperationCommand;
import com.redis.spring.batch.writer.operation.Noop;

import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;

@Command(name = "noop", description = "No operation: accepts input and does nothing")
public class NoopCommand implements OperationCommand<Map<String, Object>> {

	@Mixin
	private HelpOptions helpOptions = new HelpOptions();

	@Override
	public Noop<String, String, Map<String, Object>> operation() {
		return new Noop<>();
	}

}
