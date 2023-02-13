package com.redis.riot.command;

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
		return Expire.<String, Map<String, Object>>key(key())
				.<String>millis(numberExtractor(options.getTimeoutField(), Long.class, options.getTimeoutDefault()))
				.build();
	}

}
