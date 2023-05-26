package com.redis.riot.cli.operation;

import java.util.Map;

import com.redis.spring.batch.writer.operation.Expire;

import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;

@Command(name = "expire", description = "Set timeouts on keys")
public class ExpireCommand extends AbstractKeyCommand {

	@Mixin
	private ExpireOptions options = new ExpireOptions();

	@Override
	public Expire<String, String, Map<String, Object>> operation() {
		return new Expire<>(key(), numberExtractor(options.getTtlField(), Long.class, options.getDefaultTtl()));
	}

}
