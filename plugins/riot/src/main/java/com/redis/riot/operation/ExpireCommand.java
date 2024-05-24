package com.redis.riot.operation;

import java.util.Map;

import com.redis.spring.batch.item.redis.writer.operation.Expire;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "expire", description = "Set timeouts on keys")
public class ExpireCommand extends AbstractOperationCommand {

	public static final long DEFAULT_TTL = 60;

	@Option(names = "--ttl", description = "EXPIRE timeout field.", paramLabel = "<field>")
	private String ttlField;

	@Override
	public Expire<String, String, Map<String, Object>> operation() {
		Expire<String, String, Map<String, Object>> operation = new Expire<>(keyFunction());
		operation.setTtlFunction(toLong(ttlField));
		return operation;
	}

}