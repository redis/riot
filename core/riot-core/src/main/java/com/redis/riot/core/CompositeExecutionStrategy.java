package com.redis.riot.core;

import picocli.CommandLine.ExecutionException;
import picocli.CommandLine.IExecutionStrategy;
import picocli.CommandLine.ParameterException;
import picocli.CommandLine.ParseResult;

public class CompositeExecutionStrategy implements IExecutionStrategy {

	private final IExecutionStrategy inactive;
	private final IExecutionStrategy active;

	public CompositeExecutionStrategy(IExecutionStrategy inactive, IExecutionStrategy active) {
		this.inactive = inactive;
		this.active = active;
	}

	@Override
	public int execute(ParseResult parseResult) throws ExecutionException, ParameterException {
		inactive.execute(parseResult);
		return active.execute(parseResult);
	}

}
