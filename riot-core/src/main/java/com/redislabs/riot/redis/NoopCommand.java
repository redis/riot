package com.redislabs.riot.redis;

import java.util.Map;

import picocli.CommandLine.Command;

@Command(name = "noop")
public class NoopCommand extends AbstractRedisCommand<Map<String, Object>> {

	@Override
	public Noop<String, String, Map<String, Object>> writer() {
		return new Noop<>();
	}

}
