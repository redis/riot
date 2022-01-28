package com.redis.riot.redis;

import java.util.Map;

import com.redis.spring.batch.writer.RedisOperation;
import com.redis.spring.batch.writer.operation.Rpush;

import picocli.CommandLine.Command;

@Command(name = "rpush", description = "Insert values at the tail of a list")
public class RpushCommand extends AbstractCollectionCommand {

	@Override
	public RedisOperation<String, String, Map<String, Object>> operation() {
		return Rpush.<String, String, Map<String, Object>>key(key()).member(member()).build();
	}

}
