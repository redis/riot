package com.redis.riot.core.operation;

import java.util.Map;
import java.util.function.Function;

import com.redis.spring.batch.writer.operation.Xadd;

import io.lettuce.core.XAddArgs;

public class XaddSupplier extends AbstractFilterMapOperationBuilder {

	private long maxlen;
	private boolean approximateTrimming;

	public void setMaxlen(long maxlen) {
		this.maxlen = maxlen;
	}

	public void setApproximateTrimming(boolean approximateTrimming) {
		this.approximateTrimming = approximateTrimming;
	}

	@Override
	protected Xadd<String, String, Map<String, Object>> operation(Function<Map<String, Object>, String> keyFunction) {
		Xadd<String, String, Map<String, Object>> operation = new Xadd<>(keyFunction, map());
		operation.setArgs(args());
		return operation;
	}

	private XAddArgs args() {
		XAddArgs args = new XAddArgs();
		if (maxlen > 0) {
			args.maxlen(maxlen);
		}
		args.approximateTrimming(approximateTrimming);
		return args;
	}

}
