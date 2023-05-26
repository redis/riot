package com.redis.riot.cli.operation;

import java.util.Map;

import com.redis.spring.batch.writer.operation.Rpush;

import picocli.CommandLine.Command;

@Command(name = "rpush", description = "Insert values at the tail of a list")
public class RpushCommand extends AbstractCollectionCommand {

	@Override
	public Rpush<String, String, Map<String, Object>> operation() {
		return new Rpush<>(key(), member());
	}

}
