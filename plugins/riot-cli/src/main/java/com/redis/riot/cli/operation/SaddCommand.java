package com.redis.riot.cli.operation;

import java.util.Map;

import com.redis.spring.batch.writer.operation.Sadd;

import picocli.CommandLine.Command;

@Command(name = "sadd", description = "Add members to a set")
public class SaddCommand extends AbstractCollectionCommand {

	@Override
	public Sadd<String, String, Map<String, Object>> operation() {
		return new Sadd<>(key(), member());
	}

}
