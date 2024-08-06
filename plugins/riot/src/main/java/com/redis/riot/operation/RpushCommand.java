package com.redis.riot.operation;

import java.util.Arrays;
import java.util.Map;

import com.redis.spring.batch.item.redis.writer.impl.Rpush;

import picocli.CommandLine.Command;

@Command(name = "rpush", description = "Insert values at the tail of a list")
public class RpushCommand extends AbstractMemberOperationCommand {

	@Override
	public Rpush<String, String, Map<String, Object>> operation() {
		return new Rpush<>(keyFunction(), memberFunction().andThen(Arrays::asList));
	}

}