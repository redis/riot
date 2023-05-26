package com.redis.riot.cli.operation;

import java.util.Map;

import com.redis.spring.batch.writer.operation.Xadd;

import io.lettuce.core.XAddArgs;
import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;

@Command(name = "xadd", description = "Append entries to a stream")
public class XaddCommand extends AbstractKeyCommand {

	@Mixin
	private XaddOptions options = new XaddOptions();

	@Override
	public Xadd<String, String, Map<String, Object>> operation() {
		XAddArgs args = new XAddArgs();
		options.getMaxlen().ifPresent(args::maxlen);
		args.approximateTrimming(options.isApproximateTrimming());
		return new Xadd<>(key(), options.getFilteringOptions().converter(), t -> args);
	}

}
