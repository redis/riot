package com.redis.riot.cli;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import picocli.CommandLine.ExecutionException;
import picocli.CommandLine.ExitCode;
import picocli.CommandLine.IExecutionStrategy;
import picocli.CommandLine.ParameterException;
import picocli.CommandLine.ParseResult;

public class CompositeExecutionStrategy implements IExecutionStrategy {

    private List<IExecutionStrategy> delegates = new ArrayList<>();

    public void setDelegates(List<IExecutionStrategy> delegates) {
        this.delegates = delegates;
    }

    public void addDelegates(IExecutionStrategy... executionStrategies) {
        this.delegates.addAll(Arrays.asList(executionStrategies));
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
