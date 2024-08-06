package com.redis.riot.operation;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.function.Function;

import com.redis.spring.batch.item.redis.writer.impl.Xadd;

import io.lettuce.core.StreamMessage;
import io.lettuce.core.XAddArgs;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "xadd", description = "Append entries to a stream")
public class XaddCommand extends AbstractOperationCommand {

	@ArgGroup(exclusive = false)
	private FieldFilterArgs fieldFilterArgs = new FieldFilterArgs();

	@Option(names = "--maxlen", description = "Stream maxlen.", paramLabel = "<int>")
	private long maxlen;

	@Option(names = "--trim", description = "Stream efficient trimming ('~' flag).")
	private boolean approximateTrimming;

	private XAddArgs xAddArgs() {
		XAddArgs args = new XAddArgs();
		if (maxlen > 0) {
			args.maxlen(maxlen);
		}
		args.approximateTrimming(approximateTrimming);
		return args;
	}

	@Override
	public Xadd<String, String, Map<String, Object>> operation() {
		Xadd<String, String, Map<String, Object>> operation = new Xadd<>(keyFunction(), messageFunction());
		operation.setArgs(xAddArgs());
		return operation;
	}

	private Function<Map<String, Object>, Collection<StreamMessage<String, String>>> messageFunction() {
		Function<Map<String, Object>, String> keyFunction = keyFunction();
		Function<Map<String, Object>, Map<String, String>> mapFunction = fieldFilterArgs.mapFunction();
		return m -> Arrays.asList(new StreamMessage<>(keyFunction.apply(m), null, mapFunction.apply(m)));
	}

	public FieldFilterArgs getFieldFilterArgs() {
		return fieldFilterArgs;
	}

	public void setFieldFilterArgs(FieldFilterArgs args) {
		this.fieldFilterArgs = args;
	}

	public long getMaxlen() {
		return maxlen;
	}

	public void setMaxlen(long maxlen) {
		this.maxlen = maxlen;
	}

	public boolean isApproximateTrimming() {
		return approximateTrimming;
	}

	public void setApproximateTrimming(boolean approximateTrimming) {
		this.approximateTrimming = approximateTrimming;
	}

}