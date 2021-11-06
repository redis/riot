package com.redis.riot.redis;

import java.util.Map;

import com.redis.spring.batch.support.RedisOperation;
import com.redis.spring.batch.support.operation.Xadd;

import io.lettuce.core.XAddArgs;
import lombok.EqualsAndHashCode;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@EqualsAndHashCode(callSuper = true)
@Command(name = "xadd", description = "Append entries to a stream")
public class XaddCommand extends AbstractKeyCommand {

	@CommandLine.Mixin
	private FilteringOptions filteringOptions = new FilteringOptions();
	@Option(names = "--maxlen", description = "Stream maxlen", paramLabel = "<int>")
	private Long maxlen;
	@Option(names = "--trim", description = "Stream efficient trimming ('~' flag)")
	private boolean approximateTrimming;

	@Override
	public RedisOperation<String, String, Map<String, Object>> operation() {
		XAddArgs args = new XAddArgs();
		if (maxlen != null) {
			args.maxlen(maxlen);
		}
		args.approximateTrimming(approximateTrimming);
		return Xadd.key(key()).body(filteringOptions.converter()).args(args).build();
	}

}
