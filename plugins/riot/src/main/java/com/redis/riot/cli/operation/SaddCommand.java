package com.redis.riot.cli.operation;

import java.util.Map;

import com.redis.spring.batch.writer.operation.Sadd;

import picocli.CommandLine.Command;

@Command(name = "sadd", description = "Add members to a set")
public class SaddCommand extends AbstractCollectionCommand {

	@Override
	public Sadd<String, String, Map<String, Object>> operation() {
		return Sadd.<String, Map<String, Object>>key(key()).member(member()).build();
	}

}
