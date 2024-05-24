package com.redis.riot.core;

import java.util.List;

import picocli.CommandLine.ExecutionException;
import picocli.CommandLine.ExitCode;
import picocli.CommandLine.IExecutionStrategy;
import picocli.CommandLine.ParameterException;
import picocli.CommandLine.ParseResult;

public class CompositeExecutionStrategy implements IExecutionStrategy {

	private final List<IExecutionStrategy> delegates;

	public CompositeExecutionStrategy(List<IExecutionStrategy> delegates) {
		this.delegates = delegates;
	}

	@Override
	public int execute(ParseResult parseResult) throws ExecutionException, ParameterException {
		for (IExecutionStrategy delegate : delegates) {
			int result = delegate.execute(parseResult);
			if (result != ExitCode.OK) {
				return result;
			}
		}
		return ExitCode.OK;
	}

}
