package com.redis.riot.redis;

import java.util.Map;

import com.redis.spring.batch.writer.operation.Lpush;

import picocli.CommandLine.Command;

@Command(name = "lpush", description = "Insert values at the head of a list")
public class LpushCommand extends AbstractCollectionCommand {

	@Override
	public Lpush<String, String, Map<String, Object>> operation() {
		return Lpush.<String, Map<String, Object>>key(key()).member(member()).build();
	}

}
