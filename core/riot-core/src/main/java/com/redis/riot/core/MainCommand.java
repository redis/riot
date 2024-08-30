package com.redis.riot.core;

import java.io.PrintWriter;
import java.util.concurrent.Callable;

import org.springframework.util.unit.DataSize;

import com.redis.spring.batch.Range;

import picocli.CommandLine;
import picocli.CommandLine.IExecutionStrategy;
import picocli.CommandLine.RunLast;

public class MainCommand extends BaseCommand implements Callable<Integer>, IO {

	private PrintWriter out;
	private PrintWriter err;

	@Override
	public PrintWriter getOut() {
		return out;
	}

	@Override
	public void setOut(PrintWriter out) {
		this.out = out;
	}

	@Override
	public PrintWriter getErr() {
		return err;
	}

	@Override
	public void setErr(PrintWriter err) {
		this.err = err;
	}

	@Override
	public Integer call() throws Exception {
		commandSpec.commandLine().usage(out);
		return 0;
	}

	protected CommandLine commandLine() {
		CommandLine commandLine = new CommandLine(this);
		setOut(commandLine.getOut());
		setErr(commandLine.getErr());
		commandLine.setCaseInsensitiveEnumValuesAllowed(true);
		commandLine.setUnmatchedOptionsAllowedAsOptionParameters(false);
		commandLine.setExecutionExceptionHandler(new PrintExceptionMessageHandler());
		commandLine.registerConverter(DataSize.class, DataSize::parse);
		commandLine.registerConverter(Range.class, Range::parse);
		commandLine.registerConverter(Expression.class, Expression::parse);
		commandLine.registerConverter(TemplateExpression.class, Expression::parseTemplate);
		commandLine.setExecutionStrategy(LoggingMixin.executionStrategy(executionStrategy()));
		return commandLine;
	}

	protected IExecutionStrategy executionStrategy() {
		return new RunLast();
	}

	public int run(String... args) {
		return commandLine().execute(args);
	}

}
