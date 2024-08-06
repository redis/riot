package com.redis.riot.operation;

import java.util.Arrays;
import java.util.Map;

import com.redis.spring.batch.item.redis.writer.impl.Sadd;

import picocli.CommandLine.Command;

@Command(name = "sadd", description = "Add members to a set")
public class SaddCommand extends AbstractMemberOperationCommand {

	@Override
	public Sadd<String, String, Map<String, Object>> operation() {
		return new Sadd<>(keyFunction(), memberFunction().andThen(Arrays::asList));
	}

}